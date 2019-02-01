pipeline {
  agent {
    docker { image 'continuumio/anaconda3' }
  }
  stages {
    stage('pip_deps') {
      steps {
        sh 'pip install absl-py slackclient websockets pandas'
      }
    }
    stage('run_unit_test') {
      steps {
        sh 'python -m test'
      }
    }
  }
}
