pipeline {
  agent {
    docker {
      image 'continuumio/anaconda3'
    }

  }
  stages {
    stage('pip_deps') {
      steps {
        sh 'pip install --user absl-py slackclient websockets pandas'
      }
    }
    stage('run_unit_test') {
      steps {
        sh '''which python
python -m test'''
      }
    }
  }
}