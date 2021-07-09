pipeline {

    agent any
    parameters {
        choice(description: '', name: 'scope', choices: ["sales", "stocks_delta", "stocks_full", "historic_stocks"])
        choice(description: '', name: 'run_env', choices:'dev\nprod')
        string(description: 'branch name', name: 'branch_name', defaultValue:'master')
    }

    stages {
        stage("unit tests") {
            when {
                expression { params.run_env != 'prod' }
            }
            steps {
                script {
                    def testImage = docker.build("py-unit-test-image", "./docker/ --build-arg http_proxy=${https_proxy} --build-arg https_proxy=${https_proxy} --env PYTHONPATH=spark/src/")
                    testImage.inside('-u root -e PYTHONDONTWRITEBYTECODE=1') {
                        sh 'cd ${WORKSPACE}'
                        sh 'python3 -m unittest spark/test/*py'
                    }
                }
            }
        }

        stage("cluster provisioning") {

            steps {

            build job: "EMR-CREATE-PERSISTENT-CLUSTER",
                parameters: [
                    string(name: "nameOfCluster", value: "${BUILD_TAG}"),
                    string(name: "projectTag", value: "forecastinfra"),
                    string(name: "versionEMR", value: "emr-5.26.0"),
                    string(name: "instanceTypeMaster", value: "c5.4xlarge"),
                    string(name: "masterNodeDiskSize", value: "128"),
                    string(name: "nbrCoreOnDemand", value: "7"),
                    string(name: "nbrCoreSpot", value: "0"),
                    string(name: "instanceTypeCore", value: "r5.8xlarge"),
                    string(name: "coreNodeDiskSize", value: "128"),
                    string(name: "nbrTaskNode", value: "0"),
                    string(name: "instanceTypeTask", value: "c4.4xlarge"),
                    string(name: "taskNodeDiskSize", value: "64"),
                    string(name: "ldapUser", value: "wdesmarescaux"),
                    string(name: "ldapGroup", value: "GR-DISCOVERY-ADM"),
                    string(name: "hdfsReplicationFactor", value: "3")
                    ]
            }

        }

        stage("spark app deployment and execution") {
            steps {
                wrap([$class: "BuildUser"]) {
                    sh('''

                    export https_proxy="${https_proxy}"

                    EMRName="forecast-emr-${BUILD_TAG}"

                    cluster_id=$(aws emr list-clusters --active --output=json | jq '.Clusters[] | select(.Name=="'${EMRName}'") | .Id ' -r)

                    instance_fleet_id=$(aws emr describe-cluster --cluster-id ${cluster_id} --output=json | jq '.Cluster.InstanceFleets[] | select(.InstanceFleetType=="MASTER") | .Id ' -r)

                    master_ip=$(aws emr list-instances --cluster-id ${cluster_id} --output=json | jq '.Instances[] | select(.InstanceFleetId=="'${instance_fleet_id}'") | .PrivateIpAddress ' -r)

                    scp -r -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} ${WORKSPACE} hadoop@${master_ip}:/home/hadoop

                    ssh hadoop@${master_ip} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} "export PYSPARK_PYTHON='/usr/bin/python3'; sudo chmod 755 /home/hadoop/${JOB_NAME}/main_spark.sh; cd /home/hadoop/${JOB_NAME}; ./main_spark.sh ${run_env} ${scope}"
                    x=$(ssh hadoop@${master_ip} -o StrictHostKeyChecking=no -o UserKnownHostsFile=/dev/null -i /var/lib/jenkins/.ssh/${key_pem} "cat /home/hadoop/${JOB_NAME}/code_status")
                    exit $x
                    ''')
                }
            }
        }

    }

    post {
        always{
             build job: 'EMR-DELETE-PERSISTENT-CLUSTER',
                parameters: [
                string(name: 'nameOfCluster', value: "${BUILD_TAG}")]
        }
        failure {
            mail to: "noreply-forecastunited@decathlon.com",
            subject: "Pipeline ${JOB_NAME} failed", body: "${BUILD_URL}"
        }
        unstable {
            mail to: "noreply-forecastunited@decathlon.com",
            subject: "Pipeline ${JOB_NAME} unstable", body: "${BUILD_URL}"
        }
    }
}