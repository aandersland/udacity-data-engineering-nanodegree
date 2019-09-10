import pandas as pd
import boto3
import json
import configparser
import psycopg2
import datetime
import time
import sys
from botocore.exceptions import ClientError


def create_aws_client_resource(_client_resource, _type, _region, _key,
                               _secret):
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
    :param _s3: S3 resource
    :param _bucket: S3 bucket
    :param _prefix: Prefix to filter data
    """
    _s3_bucket = _s3.Bucket(_bucket)
    for _obj in _s3_bucket.objects.filter(Prefix=_prefix):
        print(_obj)


def create_aws_iam_role(_iam, _role_name, _description, _policy_arn):
    """
    Method to create an iam role in AWS
    :param _iam: IAM resource
    :param _role_name: Name of role to be created.
    :param _description: Description of the role name
    :param _policy_arn: AWS ARN
    """
    # Create the role
    try:
        print("1.1 Creating a new IAM Role")
        wfly_role = _iam.create_role(
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

    print("Attaching Policy")
    _iam.attach_role_policy(RoleName=_role_name,
                            PolicyArn=_policy_arn
                            )['ResponseMetadata']['HTTPStatusCode']

    print("Get the IAM role ARN")
    _role_arn = _iam.get_role(RoleName=_role_name)['Role']['Arn']
    print(_role_arn)

    return _role_arn


def create_redshift_cluster(_redshift, _cluster_type, _node_type, _num_nodes,
                            _cluster_identifier, _db_name, _db_user,
                            _db_password, _role_arn):
    """
    Method to create an AWS redshift cluster
    :param _redshift: Redshift client
    :param _cluster_type: Type of cluster
    :param _node_type: Type of node
    :param _num_nodes: Number of nodes
    :param _cluster_identifier: Unique name to assign
    :param _db_name: Name of database
    :param _db_user: Name of user
    :param _db_password: Password of user
    :param _role_arn: AWS ARN
    :return: Response from AWS create
    """
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
        print(datetime.datetime.now(),
              'Sleeping 7 minutes for cluster to be created.')
        time.sleep(420)
    except Exception as e:
        print(e)
    return _response


def pretty_redshift_props(props):
    """
    Format properties for printing
    :param props: Properties
    :return: Pandas dataframe
    """
    pd.set_option('display.max_colwidth', -1)
    keys_to_show = ["ClusterIdentifier", "NodeType", "ClusterStatus",
                    "MasterUsername", "DBName", "Endpoint", "NumberOfNodes",
                    'VpcId']
    x = [(k, v) for k, v in props.items() if k in keys_to_show]
    return pd.DataFrame(data=x, columns=["Key", "Value"])


def check_aws_cluster_status(_redshift, _cluster_identifier):
    """
    Method to check if a redshift cluster is active
    :param _redshift: Redshift client
    :param _cluster_identifier: Name of cluster
    :return: True/False
    """
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
    """
    Method to get cluster details
    :param _redshift: Redshift client
    :param _cluster_identifier: Name of cluster
    :return: properties, aws endpoint, aws arn or None, None, None
    """
    if check_aws_cluster_status(_redshift, _cluster_identifier):
        my_cluster_props = \
            _redshift.describe_clusters(
                ClusterIdentifier=_cluster_identifier)['Clusters'][0]

        wfly_endpoint = my_cluster_props['Endpoint']['Address']

        wfly_role_arn = my_cluster_props['IamRoles'][0]['IamRoleArn']
        print(pretty_redshift_props(my_cluster_props))

        return my_cluster_props, wfly_endpoint, wfly_role_arn
    else:
        print('UNABLE TO FIND CLUSTER, MANUALLY CHECK.')
        return None, None, None


def open_aws_tcp_port_to_cluster(_ec2, _cluster_props, _port):
    """
    Method to open necessary ports for database
    :param _ec2: EC2 resource
    :param _cluster_props: Properties of cluster
    :param _port: Port to open
    """
    try:
        vpc = _ec2.Vpc(id=_cluster_props['VpcId'])
        default_sg = list(vpc.security_groups.all())[-1]

        print('SECURITY GROUP: ', default_sg)

        default_sg.authorize_ingress(
            GroupName=default_sg.group_name,
            CidrIp='0.0.0.0/0',
            IpProtocol='TCP',
            FromPort=int(_port),
            ToPort=int(_port)
        )
    except Exception as e:
        print(e)


def delete_aws_redshift_cluster(_redshift, _cluster_identifier):
    """
    Method to delete a redshift cluster
    :param _redshift: Redshift client
    :param _cluster_identifier: Cluster name
    """
    print('Deleting cluster')
    _redshift.delete_cluster(ClusterIdentifier=_cluster_identifier,
                             SkipFinalClusterSnapshot=True)


def delete_aws_iam_policy_role(_iam, _role_name, _policy_arn):
    """
    Method to delete an AWS policy and role
    :param _iam: IAM client
    :param _role_name: Name of role
    :param _policy_arn: Name of ARN
    """
    print('Deleting policy and role')
    _iam.detach_role_policy(RoleName=_role_name,
                            PolicyArn=_policy_arn)
    _iam.delete_role(RoleName=_role_name)


def create_cluster():
    """
    Method to create a AWS redshift cluster and associated resources, clients,
    and database connection.
    config = Cluster configurations
    ec2 = EC2 resource
    s3 = S3 resource
    iam = IAM client
    redshift = Redshift client
    cluster_props = Configured cluster properties
    conn = Connection to database
    cur = Cursor to database connection
    :return: config, ec2, s3, iam, redshift, cluster_props, conn, cur
    """
    config = configparser.ConfigParser()
    config.read_file(open('configs.cfg'))

    aws_key = config.get('AWS', 'KEY')
    aws_secret = config.get('AWS', 'SECRET')
    region = config.get('AWS', 'REGION')
    wfly_cluster_type = config.get("WFLY", "WFLY_CLUSTER_TYPE")
    wfly_num_nodes = config.get("WFLY", "WFLY_NUM_NODES")
    wfly_node_type = config.get("WFLY", "WFLY_NODE_TYPE")
    wfly_cluster_identifier = config.get("WFLY", "WFLY_CLUSTER_IDENTIFIER")
    wfly_db = config.get("WFLY", "WFLY_DB")
    wfly_db_user = config.get("WFLY", "WFLY_DB_USER")
    wfly_db_password = config.get("WFLY", "WFLY_DB_PASSWORD")
    wfly_port = config.get("WFLY", "WFLY_PORT")
    wfly_iam_role_name = config.get("WFLY", "WFLY_IAM_ROLE_NAME")
    policy_arn = config.get('IAM_ROLE', 'POLICY')

    # AWS resource and clients
    _ec2 = create_aws_client_resource('resource', 'ec2', region, aws_key,
                                      aws_secret)
    _s3 = create_aws_client_resource('resource', 's3', region, aws_key,
                                     aws_secret)
    _iam = create_aws_client_resource('client', 'iam', region, aws_key,
                                      aws_secret)
    _redshift = create_aws_client_resource('client', 'redshift', region,
                                           aws_key,
                                           aws_secret)

    cluster_list = _redshift.describe_clusters()
    print(cluster_list)

    _iam_role_arn = create_aws_iam_role(
        _iam, wfly_iam_role_name,
        "Allows Redshift clusters to call AWS services on your behalf.",
        policy_arn)

    create_redshift_cluster(_redshift, wfly_cluster_type, wfly_node_type,
                            wfly_num_nodes, wfly_cluster_identifier, wfly_db,
                            wfly_db_user, wfly_db_password, _iam_role_arn)

    _cluster_props, wfly_endpoint, wfly_role_arn = get_aws_cluster_properties(
        _redshift, wfly_cluster_identifier)

    # Exit if we do not have a cluster instance created. It is possible that
    # the instance is still being created. Rerun script to check again.
    if _cluster_props is None or wfly_endpoint is None or \
            wfly_role_arn is None:
        sys.exit('UNABLE TO FIND CLUSTER, MANUALLY CHECK.')

    # add to config
    config['WFLY']['WFLY_ENDPOINT'] = wfly_endpoint
    config['WFLY']['WFLY_ROLE_ARN'] = wfly_role_arn
    config['CLUSTER']['HOST'] = wfly_endpoint

    print('CLUSTER PROPS:', _cluster_props)

    open_aws_tcp_port_to_cluster(_ec2, _cluster_props, wfly_port)
    print(*config['CLUSTER'].values())

    print('Connecting to Redshift. . .')
    _conn = psycopg2.connect("host={} dbname={} user={} password={} port={}"
                             .format(*config['CLUSTER'].values()))
    _cur = _conn.cursor()

    print('Redshift connected', _conn)

    return config, _ec2, _s3, _iam, _redshift, _cluster_props, _conn, _cur


def delete_cluster(_redshift, _cluster_identifier, _iam, _iam_role_name,
                   _arn):
    """
    Method to delete a redshift cluster
    :param _redshift: Redshift client
    :param _cluster_identifier: Cluster name
    :param _iam: IAM client
    :param _iam_role_name: Role name
    :param _arn: ARN
    """
    delete_aws_redshift_cluster(_redshift, _cluster_identifier)
    delete_aws_iam_policy_role(_iam, _iam_role_name, _arn)


if __name__ == "__main__":
    print('Creating cluster is up.')
    config, ec2, s3, iam, redshift, cluster_props, conn, cur = create_cluster()
    print('Cluster is up.')
    # delete_cluster(redshift,
    #                config.get("WFLY", "WFLY_CLUSTER_IDENTIFIER"),
    #                iam, config.get("WFLY", "WFLY_IAM_ROLE_NAME"),
    #                config.get('IAM_ROLE', 'POLICY'))
    # print('Deleting cluster.')
