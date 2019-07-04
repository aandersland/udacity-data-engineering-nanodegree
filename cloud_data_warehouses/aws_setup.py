import pandas as pd
import boto3
import json
import configparser
import psycopg2
import time
import sys
from botocore.exceptions import ClientError


def create_aws_client_resource(_client_resource, _type, _region, _key, _secret):
    """
    Method to create an AWS client/resource
    :param _client_resource: Indicates a client or resource when creating
    instance
    :param _type: Type of client/resource to create (ec2, s3, iam, redshift)
    :param _region: AWS region
    :param _key: AWS user key
    :param _secret: AWS user secret
    :return: A client or resource
    """

    if _client_resource == 'resource':
        _res = boto3.resource(_type,
                              region_name=_region,
                              aws_access_key_id=_key,
                              aws_secret_access_key=_secret
                              )
    else:
        _res = boto3.client(_type,
                            region_name=_region,
                            aws_access_key_id=_key,
                            aws_secret_access_key=_secret
                            )
    return _res


def get_s3_data(_s3, _bucket, _prefix):
    """
    Method to get s3 data
    :param _bucket: S3 bucket
    :param _prefix: Prefix to filter data
    """
    _s3_bucket = _s3.Bucket(_bucket)
    for _obj in _s3_bucket.objects.filter(Prefix=_prefix):
        print(_obj)


def create_aws_iam_role(_iam, _role_name, _description, _policy_arn):
    """
    Method to create an iam role in AWS
    :param _role_name: Name of role to be created.
    :param _description: Description of the role name
    """
    # 1.1 Create the role,
    try:
        print("1.1 Creating a new IAM Role")
        dwh_role = _iam.create_role(
            Path='/',
            RoleName=_role_name,
            Description=_description,
            AssumeRolePolicyDocument=json.dumps(
                {'Statement': [{'Action': 'sts:AssumeRole',
                                'Effect': 'Allow',
                                'Principal': {
                                    'Service': 'redshift.amazonaws.com'}}],
                 'Version': '2012-10-17'})
        )
    except Exception as e:
        print(e)

    print("1.2 Attaching Policy")
    _iam.attach_role_policy(RoleName=_role_name,
                            PolicyArn=_policy_arn
                            )['ResponseMetadata']['HTTPStatusCode']

    print("1.3 Get the IAM role ARN")
    _role_arn = _iam.get_role(RoleName=_role_name)['Role']['Arn']
    print(_role_arn)

    return _role_arn


def create_redshift_cluster(_redshift, _cluster_type, _node_type, _num_nodes,
                            _cluster_identifier, _db_name, _db_user,
                            _db_password, _role_arn):
    _response = ''
    try:
        _response = _redshift.create_cluster(
            # HW
            ClusterType=_cluster_type,
            NodeType=_node_type,
            NumberOfNodes=int(_num_nodes),

            # Identifiers & Credentials
            DBName=_db_name,
            ClusterIdentifier=_cluster_identifier,
            MasterUsername=_db_user,
            MasterUserPassword=_db_password,

            # Roles (for s3 access)
            IamRoles=[_role_arn]
        )
        print('Sleeping 7 minutes for cluster to be created.')
        time.sleep(420)
    except Exception as e:
        print(e)
    return _response


def prettyRedshiftProps(props):
    pd.set_option('display.max_colwidth', -1)
    keysToShow = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                  "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                  'VpcId']
    x = [(k, v) for k, v in props.items() if k in keysToShow]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_aws_cluster_status(_redshift, _cluster_identifier):
    try:
        _clusters = _redshift.describe_clusters(
            ClusterIdentifier=_cluster_identifier)
        print('Cluster found: ', _clusters)
        return True
    except ClientError as e:
        print(e.response['Error']['Code'])
        print('Cluster not found.')
        return False


def get_aws_cluster_properties(_redshift, _cluster_identifier):
    if check_aws_cluster_status(_redshift, _cluster_identifier):
        myClusterProps = \
            _redshift.describe_clusters(ClusterIdentifier=_cluster_identifier)[
                'Clusters'][0]
        prettyRedshiftProps(myClusterProps)

        DWH_ENDPOINT = myClusterProps['Endpoint']['Address']
        print("DWH_ENDPOINT :: ", DWH_ENDPOINT)

        DWH_ROLE_ARN = myClusterProps['IamRoles'][0]['IamRoleArn']
        print("DWH_ROLE_ARN :: ", DWH_ROLE_ARN)
        return myClusterProps, DWH_ENDPOINT, DWH_ROLE_ARN
    else:
        # todo need to change this to work with etl and not exit
        print('UNABLE TO FIND CLUSTER, MANUALLY CHECK.')
        return None, None, None


def open_aws_tcp_port_to_cluster(_ec2, _cluster_props, _port):
    try:
        vpc = _ec2.Vpc(id=_cluster_props['VpcId'])
        defaultSg = list(vpc.security_groups.all())[-1]
        print('SECURITY GROUP: ', defaultSg)
        defaultSg.authorize_ingress(
            GroupName=defaultSg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(_port),
            ToPort=int(_port)
        )
    except Exception as e:
        print(e)


def delete_aws_redshift_cluster(_redshift, _cluster_identifier):
    print('Deleting cluster')
    _redshift.delete_cluster(ClusterIdentifier=_cluster_identifier,
                             SkipFinalClusterSnapshot=True)


def delete_aws_iam_policy_role(_iam, _role_name, _policy_arn):
    print('Deleting policy and role')
    _iam.detach_role_policy(RoleName=_role_name,
                            PolicyArn=_policy_arn)
    _iam.delete_role(RoleName=_role_name)


def create_cluster():
    config = configparser.ConfigParser()
    config.read_file(open('dwh.cfg'))

    KEY = config.get('AWS', 'KEY')
    SECRET = config.get('AWS', 'SECRET')
    REGION = config.get('AWS', 'REGION')

    DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
    DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
    DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")

    DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
    DWH_DB = config.get("DWH", "DWH_DB")
    DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
    DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
    DWH_PORT = config.get("DWH", "DWH_PORT")

    DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")

    POLICY_ARN = config.get('IAM_ROLE', 'POLICY')

    # (DWH_DB_USER, DWH_DB_PASSWORD, DWH_DB)

    df = pd.DataFrame({"Param":
                           ["DWH_CLUSTER_TYPE", "DWH_NUM_NODES",
                            "DWH_NODE_TYPE",
                            "DWH_CLUSTER_IDENTIFIER", "DWH_DB", "DWH_DB_USER",
                            "DWH_DB_PASSWORD", "DWH_PORT", "DWH_IAM_ROLE_NAME"],
                       "Value":
                           [DWH_CLUSTER_TYPE, DWH_NUM_NODES, DWH_NODE_TYPE,
                            DWH_CLUSTER_IDENTIFIER, DWH_DB, DWH_DB_USER,
                            DWH_DB_PASSWORD,
                            DWH_PORT, DWH_IAM_ROLE_NAME]
                       })
    print(df)

    ec2 = create_aws_client_resource('resource', 'ec2', REGION, KEY, SECRET)
    s3 = create_aws_client_resource('resource', 's3', REGION, KEY, SECRET)
    iam = create_aws_client_resource('client', 'iam', REGION, KEY, SECRET)
    redshift = create_aws_client_resource('client', 'redshift', REGION, KEY,
                                          SECRET)
    # print('Print sample S3 data')
    # get_s3_data(s3, 'awssampledbuswest2', 'ssbgz')

    cluster_list = redshift.describe_clusters()
    print(cluster_list)

    _iam_role_arn = create_aws_iam_role(
        iam, DWH_IAM_ROLE_NAME,
        "Allows Redshift clusters to call AWS services on your behalf.",
        POLICY_ARN)

    create_redshift_cluster(redshift, DWH_CLUSTER_TYPE, DWH_NODE_TYPE,
                            DWH_NUM_NODES, DWH_CLUSTER_IDENTIFIER, DWH_DB,
                            DWH_DB_USER, DWH_DB_PASSWORD, _iam_role_arn)

    cluster_props, DWH_ENDPOINT, DWH_ROLE_ARN = get_aws_cluster_properties(
        redshift, DWH_CLUSTER_IDENTIFIER)

    if cluster_props is None or DWH_ENDPOINT is None or DWH_ROLE_ARN is None:
        sys.exit('UNABLE TO FIND CLUSTER, MANUALLY CHECK.')

    # add endpoint to config
    config['DWH']['DWH_ENDPOINT'] = DWH_ENDPOINT
    config['DWH']['DWH_ROLE_ARN'] = DWH_ROLE_ARN
    config['CLUSTER']['HOST'] = DWH_ENDPOINT

    print('CLUSTER PROPS:', cluster_props)
    #
    open_aws_tcp_port_to_cluster(ec2, cluster_props, DWH_PORT)
    print(*config['CLUSTER'].values())

    print('Connecting to Redshift. . .')
    conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                            .format(*config['CLUSTER'].values()))

    cur = conn.cursor()

    print('Redshift connected', conn)

    return config, ec2, s3, iam, redshift, cluster_props, conn, cur


def delete_cluster(_redshift, _cluster_identifier, _iam, _iam_role_name,
                   _arn):
    delete_aws_redshift_cluster(_redshift, _cluster_identifier)
    delete_aws_iam_policy_role(_iam, _iam_role_name, _arn)


if __name__ == "__main__":
    config, ec2, s3, iam, redshift, cluster_props, conn, cur = create_cluster()
    print('Cluster is up.')
    delete_cluster(redshift,
                   config.get("DWH", "DWH_CLUSTER_IDENTIFIER"),
                   iam, config.get("DWH", "DWH_IAM_ROLE_NAME"),
                   config.get('IAM_ROLE', 'POLICY'))
