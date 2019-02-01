pipeline {
  agent none
  stages {
    stage('copy_credentials') {
      steps {
        sh '''cp /home/ningr/Documents/ok_bot/api_key_v3 .
cp /home/ningr/Documents/ok_bot/pass_phrase_v3 .
cp /home/ningr/Documents/ok_bot/secret_key_v3 .'''
      }
    }
    stage('run_unit_test') {
      steps {
        sh 'python -m test'
      }
    }
  }
}