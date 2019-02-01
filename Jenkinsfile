pipeline {
  agent {
    docker {
      image 'python:3.7.2-alpine3.8'
    }

  }
  stages {
    stage('pip_deps') {
      steps {
        sh '''pip install numpy scipy matplotlib ipython jupyter pandas sympy nose
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