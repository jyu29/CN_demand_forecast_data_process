pipeline {

    agent any

    environment {
        key_pem = 'forecast-emr.pem'
        cluster_name = 'forecast-data-refining-demand'
        jenkins_job = 'forecast-data-refining-demand'
    }

    stages {

        stage('cluster provisioning') {

            steps {

            build job: 'EMR-CREATE-DEV-CLUSTER',
                parameters: [
                    string(name: 'nameOfCluster', value: "${cluster_name}"),
                    string(name: 'projectTag', value: 'forecastinfra'),
                    string(name: 'versionEMR', value: 'emr-5.26.0'),
                    string(name: 'instanceTypeMaster', value: 'c5.2xlarge'),
                    string(name: 'masterNodeDiskSize', value: '64'),
                    string(name: 'nbrCoreOnDemand', value: '3'),
                    string(name: 'nbrCoreSpot', value: '0'),
                    string(name: 'instanceTypeCore', value: 'r5.4xlarge'),
                    string(name: 'coreNodeDiskSize', value: '64'),
                    string(name: 'nbrTaskNode', value: '0'),
                    string(name: 'instanceTypeTask', value: 'c4.4xlarge'),
                    string(name: 'taskNodeDiskSize', value: '64'),
                    string(name: 'ldapUser', value: 'aschwartz'),
                    string(name: 'ldapGroup', value: 'GR-DISCOVERY-ADM'),
                    string(name: 'hdfsReplicationFactor', value: '3')
                    ]
            }

        }

        stage('spark app deployment and execution') {

            steps {
                
                sh('''
                
                export https_proxy=http://proxy-internet-aws-eu.subsidia.org:3128
                
                EMRName=$"forecast-dev-emr-${cluster_name}-${BUILD_USER}"
                
                cluster_id=$(aws emr list-clusters --active  --output=json | jq '.Clusters[] | select(.Name=="'${EMRName}'") | .Id ' -r)
                
                instance_fleet_id=$(aws emr describe-cluster --cluster-id ${cluster_id}  --output=json | jq '.Cluster.InstanceFleets[] | select(.InstanceFleetType=="MASTER") | .Id ' -r)
                
                master_ip=$(aws emr list-instances --cluster-id ${cluster_id}   --output=json | jq '.Instances[] | select(.InstanceFleetId=="'${instance_fleet_id}'") | .PrivateIpAddress ' -r)

                scp -r -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} ${WORKSPACE} hadoop@${master_ip}:/home/hadoop

                ssh hadoop@${master_ip} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} "sudo chmod 755 /home/hadoop/${jenkins_job}/spark_submit_global.sh /home/hadoop/${jenkins_job}/spark_submit_specific.sh; export PYSPARK_PYTHON='/usr/bin/python3'; cd ${jenkins_job}; ./spark_submit_global.sh ${runenvironment}; ./spark_submit_specific.sh ${runenvironment}"
                ''')
            }

            }

        }

        stage('delete cluster') {
            steps {
                build job: 'EMR-DELETE-DEV-CLUSTER'
            }
        }
    }
}
