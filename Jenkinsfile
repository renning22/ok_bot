pipeline {
  agent {
    docker {
      image 'continuumio/anaconda3'
    }

  }
  stages {
    stage('pip_deps') {
      steps {
        sh '''virtualenv .venv
source .venv/bin/activate
pip install absl-py slackclient websockets pandas'''
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