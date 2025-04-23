import os
from dotenv import load_dotenv
from smtp import SparkPostSMTPSender
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('test_smtp')

def main():
    # Load environment variables
    load_dotenv()
    
    # Get SparkPost API key from environment
    api_key = os.getenv('SPARKPOST_API_KEY')
    if not api_key:
        raise ValueError("SPARKPOST_API_KEY environment variable not set")
    
    # Get sender email from environment or use default
    from_email = os.getenv('FROM_EMAIL', 'your-verified-sender@yourdomain.com')
    
    # Initialize the sender
    sender = SparkPostSMTPSender(
        api_key=api_key,
        from_email=from_email,
        max_connections=10,
        messages_per_connection=100
    )
    
    # Create a test batch of emails
    test_emails = [
        {
            "to_email": f"recipient{i}@example.com",
            "subject": f"Test Email {i}",
            "text_content": f"This is test email {i} plain text content",
            "html_content": f"<html><body><h1>Test Email {i}</h1><p>This is the HTML content.</p></body></html>",
            "custom_headers": {
                "X-Campaign-ID": "test-campaign",
                "X-Template-ID": f"template-{i % 5}"
            }
        }
        for i in range(1, 11)  # Generate 10 test emails
    ]
    
    # Send the emails
    logger.info("Starting email sending test...")
    results = sender.send_emails(test_emails, batch_size=5)
    
    # Print results
    logger.info(f"Test complete!")
    logger.info(f"Total emails: {results['total_emails']}")
    logger.info(f"Successfully sent: {results['successfully_sent']}")
    logger.info(f"Failed: {results['failed']}")
    logger.info(f"Rate: {results['emails_per_second']:.2f} emails/second")
    
    if results['failed'] > 0:
        logger.warning("Failed emails:")
        for i, failure in enumerate(results['failed_details'], 1):
            logger.warning(f"{i}. To: {failure['email_data']['to_email']} - Error: {failure['error']}")

if __name__ == "__main__":
    main() 