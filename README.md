# SMTP Performance Test Tool

A Python tool for testing SMTP performance with SparkPost, featuring:
- Concurrent SMTP connections
- Message pipelining
- Batch sending
- Performance metrics
- Error handling and reporting

## Installation

1. Clone this repository:
```bash
git clone https://github.com/cadams500/smtp-perf-test.git
cd smtp-perf-test
```

2. Create a virtual environment (recommended):
```bash
python -m venv venv
source venv/bin/activate  # On Windows: venv\Scripts\activate
```

3. Install the package:
```bash
pip install -e .
```

## Configuration

1. Copy the example environment file:
```bash
cp .env.example .env
```

2. Edit `.env` and set your SparkPost credentials:
- `SPARKPOST_API_KEY`: Your SparkPost API key with SMTP privileges
- `FROM_EMAIL`: Your verified sender email address

## Usage

Run the test script:
```bash
python test_smtp.py
```

The script will:
1. Send 10 test emails
2. Use 10 concurrent connections
3. Process emails in batches of 5
4. Display performance metrics and any errors

## Customization

You can modify the test parameters in `test_smtp.py`:
- Number of test emails
- Batch size
- Number of concurrent connections
- Messages per connection

## Requirements

- Python 3.7+
- SparkPost account with SMTP privileges
- Verified sender domain in SparkPost 