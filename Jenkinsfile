pipeline {
    agent any
    tools {
        maven 'Default'
        jdk 'Default'
    }
    stages {
        stage ('Initialize') {
            steps {
                sh '''
                    echo "PATH = ${PATH}"
                    echo "M2_HOME = ${M2_HOME}"
                ''' 
            }
        }

        stage ('Build') {
            steps {
                sh '''
                    cd DataManagement
                    mvn -Dmaven.test.failure.ignore=true clean package
                '''
            }
            post {
                success {
                    echo "SUCCESS"
                    echo "Deploying app server"
                    sh '''
                        cp -r DataManagement/CommandLine/target/CommandLine-1.0-SNAPSHOT-server/lib /var/lib/jenkins/homesensors/
                    '''
                    echo 'Done refreshing app server (BUILD_ID added)'
                }
            }        
        }
    }
}
