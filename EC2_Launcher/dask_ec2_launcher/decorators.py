# dask_ec2_launcher/decorators.py
import boto3
from fabric import Connection
from dask.distributed import Client


def dask_function(vm_type="t2.medium", region="us-west-2", cpu=None, memory=None, vpc_id=None, subnet_id=None,
                  ami="ami-0c55b159cbfafe1f0", key_name=None, security_group_id=None, instance_profile=None,
                  placement=None, user_data=None):
    """
    Decorator to launch Dask on AWS EC2 instances and run the decorated function with Dask distributed.
    """

    def decorator(func):
        def wrapper(*args, **kwargs):
            # Step 1: Launch EC2 Instances
            ec2_client = boto3.client('ec2', region_name=region)
            num_instances = kwargs.get('num_instances', 1)

            # Define instance launch parameters
            instance_params = {
                'ImageId': ami,
                'InstanceType': vm_type,
                'MinCount': num_instances,
                'MaxCount': num_instances,
                'KeyName': key_name,
                'SecurityGroupIds': [security_group_id],
                'SubnetId': subnet_id,
                'TagSpecifications': [{
                    'ResourceType': 'instance',
                    'Tags': [{'Key': 'Name', 'Value': 'Dask Worker'}]
                }]
            }

            # Optional parameters
            if instance_profile:
                instance_params['IamInstanceProfile'] = {'Name': instance_profile}
            if placement:
                instance_params['Placement'] = {'GroupName': placement}
            if user_data:
                instance_params['UserData'] = user_data

            # Launch EC2 instances
            instances = ec2_client.run_instances(**instance_params)
            instance_ids = [instance['InstanceId'] for instance in instances['Instances']]
            print(f"Launched EC2 instances: {instance_ids}")

            # Wait for instances to be in 'running' state
            ec2_client.get_waiter('instance_running').wait(InstanceIds=instance_ids)

            # Get the public DNS names of the instances
            reservations = ec2_client.describe_instances(InstanceIds=instance_ids)['Reservations']
            instance_dns_list = [i['Instances'][0]['PublicDnsName'] for i in reservations]

            # Step 2: Set up Dask Workers on EC2 Instances
            def setup_dask_worker(instance_dns, dask_scheduler_ip):
                conn = Connection(host=instance_dns, user='ec2-user',
                                  connect_kwargs={"key_filename": f"{key_name}.pem"})

                # Install Dask and start Dask worker
                conn.run('sudo apt update && sudo apt install -y python3-pip')
                conn.run('pip3 install dask distributed')
                conn.run(f'dask-worker {dask_scheduler_ip}:8786 &', pty=False)

            # Assume that the first instance will be the scheduler
            dask_scheduler_ip = instance_dns_list[0]
            for instance_dns in instance_dns_list[1:]:
                setup_dask_worker(instance_dns, dask_scheduler_ip)

            # Step 3: Connect to the Dask Scheduler
            client = Client(f'tcp://{dask_scheduler_ip}:8786')

            # Execute the decorated function with Dask
            result = func(*args, client=client, **kwargs)

            # Step 4: Shut down EC2 instances after the task is completed
            ec2_client.terminate_instances(InstanceIds=instance_ids)
            print(f"Terminated EC2 instances: {instance_ids}")

            return result

        return wrapper

    return decorator