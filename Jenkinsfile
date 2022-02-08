// There are two adjusting here:
// 1. stage("cluster provisioning"): original EMRName="forecast-emr-${BUILD_TAG}"
// 2. stage("bigquery SQL"): string(name: "ClusterType", value: "batch_cluster")

pipeline {

    agent any

    parameters {
        choice(description: '', name: 'scope', choices: ['choices', 'sales', 'stocks_delta', 'stocks_full', 'historic_stocks'])
        choice(description: '', name: 'run_env', choices:['dev','prod','debug'])
        string(description: 'branch name', name: 'branch_name', defaultValue:'master')
    }

    stages {

        stage("cluster provisioning") {
            steps {
            build job: "EMR-CREATE-DEV-CLUSTER-V2",
                parameters: [
                    string(name: "nameOfCluster", value: "${BUILD_TAG}"),
                    string(name: "versionEMR", value: "emr-6.4.0"),
                    string(name: "ClusterType", value: "dev_cluster"),
                    string(name: "instanceTypeMaster", value: "c5.24xlarge"),
                    string(name: "masterNodeDiskSize", value: "256"),
                    string(name: "nbrCoreOnDemand", value: "6"),
                    string(name: "nbrCoreSpot", value: "0"),
                    string(name: "instanceTypeCore", value: "r5.24xlarge"),
                    string(name: "coreNodeDiskSize", value: "256"),
                    string(name: "nbrTaskNode", value: "0"),
                    string(name: "instanceTypeTask", value: "r5.2xlarge"),
                    string(name: "taskNodeDiskSize", value: "64"),
                    string(name: "hdfsReplicationFactor", value: "3")
                    ]
            }
        }

        stage("spark app deployment and execution") {
            steps {
                wrap([$class: "BuildUser"]) {
                    sh('''
                    export https_proxy="${https_proxy}"
                    EMRName="forecast-dev-emr-${BUILD_TAG}-Z21HTSAI"

                    cluster_id=$(aws emr list-clusters --active --output=json | jq '.Clusters[] | select(.Name=="'${EMRName}'") | .Id ' -r)
                    echo 'Cluster ID => ${cluster_id}'

                    instance_fleet_id=$(aws emr describe-cluster --cluster-id ${cluster_id} --output=json | jq '.Cluster.InstanceFleets[] | select(.InstanceFleetType=="MASTER") | .Id ' -r)
                    echo 'Instance Fleet ID => ${instance_fleet_id}'

                    master_ip=$(aws emr list-instances --cluster-id ${cluster_id} --output=json | jq '.Instances[] | select(.InstanceFleetId=="'${instance_fleet_id}'") | .PrivateIpAddress ' -r)
                    echo 'Master IP => ${master_ip}'

                    scp -r -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} ${WORKSPACE} hadoop@${master_ip}:/home/hadoop
                    ssh hadoop@${master_ip} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} "export PYSPARK_PYTHON='/usr/bin/python3'; sudo chmod 755 /home/hadoop/${JOB_NAME}/spark_submit_refining_global.sh; cd /home/hadoop/${JOB_NAME}; ./spark_submit_refining_global.sh ${run_env}"
                    x=$(ssh hadoop@${master_ip} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} "cat /home/hadoop/${JOB_NAME}/code_status")
                    exit $x
                    ''')
                }
            }
        }

    }

    post {
        always{
             build job: 'EMR-DELETE-DEV-CLUSTER',
                parameters: [
                string(name: 'nameOfCluster', value: "${BUILD_TAG}")]
        }
        failure {
            mail to: "hank.tsai.partner@decathlon.com",
            subject: "Pipeline ${JOB_NAME} failed", body: "${BUILD_URL}"
        }
        unstable {
            mail to: "hank.tsai.partner@decathlon.com",
            subject: "Pipeline ${JOB_NAME} unstable", body: "${BUILD_URL}"
        }
    }

}