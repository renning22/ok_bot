# OKBot

### Install Anaconda3
https://www.anaconda.com/download/

### Pip dependencies
```sh
conda install python=3.7.2
pip install absl-py ccxt slackclient websockets pandas
```

### Clone
```sh
git clone https://github.com/renning22/ok_bot.git
```

### Run

#### Log to screen
```sh
python -m ok_bot
```

#### Log to file ("log/" folder)
```sh
python -m ok_bot --logtofile
```

#### Also log to slack
Send full absl to slack
```sh
python -m ok_bot --alsologtoslack
```

Only log transactions to slack
```sh
python -m ok_bot --log_transaction_to_slack
```

### Unit Test
Run all unit tests
```sh
python -m test
```

Run individual test class
```sh
python -m test.test_arbitrage_execution
```

Run all unit tests and dump the report to XML.
```sh
python -m test -xml_output_file='1.xml'
```
The report can be sent to Jenkins later.

### More
* [Data](https://drive.google.com/open?id=1KwQDKQq31hzxEDAllOaH9rVQP7PL2eM_)
* [Meeting notes](https://paper.dropbox.com/doc/OK-Arbitrage-Meeting-Note--ASKaOlHQlfZ3PulilxnQfsNwAQ-qRg4c0Oou3OAp4c2eC8Vh)
* [Slack chat channel](https://chivesharvester.slack.com/messages/CEAFYFFFA/convo/CEAFYFFFA-1543129958.008600/)
* [Slack notification channel](https://chivesharvester.slack.com/messages/CC3CCUW65/convo/CEAFYFFFA-1543129958.008600/)
* [Dashboard](http://teb.ai)
