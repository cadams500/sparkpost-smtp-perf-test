import smtplib
import ssl
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
import concurrent.futures
import time
import logging
from typing import List, Dict, Any, Optional

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('sparkpost_smtp')

class SparkPostSMTPSender:
    def __init__(
        self,
        api_key: str,
        host: str = 'smtp.sparkpostmail.com',
        port: int = 587,
        max_connections: int = 10,
        messages_per_connection: int = 100,
        from_email: str = 'your-verified-sender@yourdomain.com',
        use_tls: bool = True
    ):
        """Initialize the SparkPost SMTP sender with configuration parameters.
        
        Args:
            api_key: SparkPost API key with SMTP privileges
            host: SparkPost SMTP host
            port: SparkPost SMTP port
            max_connections: Maximum number of concurrent SMTP connections
            messages_per_connection: Number of messages to send per connection before refreshing
            from_email: Verified sender email address
            use_tls: Whether to use TLS encryption (strongly recommended)
        """
        self.api_key = api_key
        self.host = host
        self.port = port
        self.max_connections = max_connections
        self.messages_per_connection = messages_per_connection
        self.from_email = from_email
        self.use_tls = use_tls

    def create_smtp_connection(self) -> smtplib.SMTP:
        """Create and return an authenticated SMTP connection with pipelining enabled."""
        # Create SMTP connection
        smtp = smtplib.SMTP(self.host, self.port)
        
        # Enable debug if needed
        # smtp.set_debuglevel(1)
        
        # Identify ourselves to the SMTP server
        smtp.ehlo_or_helo_if_needed()
        
        # Check if pipelining is supported
        has_pipelining = smtp.has_extn('pipelining')
        if has_pipelining:
            logger.info("SMTP server supports pipelining")
        else:
            logger.warning("SMTP server does not support pipelining")
            
        # Start TLS if requested
        if self.use_tls:
            context = ssl.create_default_context()
            smtp.starttls(context=context)
            smtp.ehlo()  # Need to re-identify after TLS
            
        # Authenticate with SparkPost SMTP credentials
        smtp.login('SMTP_Injection', self.api_key)
        
        return smtp
        
    def create_message(self, to_email: str, subject: str, text_content: str, 
                      html_content: Optional[str] = None, 
                      custom_headers: Optional[Dict[str, str]] = None) -> MIMEMultipart:
        """Create an email message with the specified content."""
        msg = MIMEMultipart('alternative')
        msg['Subject'] = subject
        msg['From'] = self.from_email
        msg['To'] = to_email
        
        # Add custom headers if provided
        if custom_headers:
            for key, value in custom_headers.items():
                msg[key] = value
                
        # Attach text and HTML parts
        msg.attach(MIMEText(text_content, 'plain'))
        if html_content:
            msg.attach(MIMEText(html_content, 'html'))
            
        return msg
        
    def send_batch(self, batch: List[Dict[str, Any]]) -> tuple:
        """Send a batch of messages through a single SMTP connection."""
        sent_count = 0
        failed_messages = []
        
        try:
            # Create a new connection for this batch
            smtp = self.create_smtp_connection()
            
            for i, email_data in enumerate(batch):
                try:
                    # Create the message
                    msg = self.create_message(
                        to_email=email_data['to_email'],
                        subject=email_data['subject'],
                        text_content=email_data['text_content'],
                        html_content=email_data.get('html_content'),
                        custom_headers=email_data.get('custom_headers')
                    )
                    
                    # Send the message
                    smtp.send_message(msg)
                    sent_count += 1
                    
                    # Refresh connection after messages_per_connection
                    if (i + 1) % self.messages_per_connection == 0 and i + 1 < len(batch):
                        smtp.quit()
                        smtp = self.create_smtp_connection()
                        logger.info(f"Refreshed SMTP connection after {self.messages_per_connection} messages")
                        
                except Exception as e:
                    logger.error(f"Failed to send message to {email_data['to_email']}: {str(e)}")
                    failed_messages.append({
                        'email_data': email_data,
                        'error': str(e)
                    })
                    
            # Clean up connection
            smtp.quit()
            
        except Exception as e:
            logger.error(f"Batch sending error: {str(e)}")
            # Mark all remaining messages in batch as failed
            for email_data in batch[sent_count:]:
                failed_messages.append({
                    'email_data': email_data,
                    'error': f"Batch error: {str(e)}"
                })
                
        return sent_count, failed_messages
        
    def send_emails(self, emails: List[Dict[str, Any]], batch_size: int = 50) -> Dict[str, Any]:
        """Send emails using multiple concurrent connections for optimal throughput.
        
        Args:
            emails: List of email data dictionaries with keys:
                   to_email, subject, text_content, html_content (optional), custom_headers (optional)
            batch_size: Number of emails to process in each batch
            
        Returns:
            Dictionary with results summary
        """
        start_time = time.time()
        total_sent = 0
        all_failed = []
        
        # Split emails into batches
        batches = [emails[i:i + batch_size] for i in range(0, len(emails), batch_size)]
        logger.info(f"Sending {len(emails)} emails in {len(batches)} batches using {min(self.max_connections, len(batches))} concurrent connections")
        
        # Use a thread pool to send batches concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=self.max_connections) as executor:
            futures = [executor.submit(self.send_batch, batch) for batch in batches]
            
            for future in concurrent.futures.as_completed(futures):
                sent, failed = future.result()
                total_sent += sent
                all_failed.extend(failed)
                
        elapsed_time = time.time() - start_time
        rate = total_sent / elapsed_time if elapsed_time > 0 else 0
        
        results = {
            'total_emails': len(emails),
            'successfully_sent': total_sent,
            'failed': len(all_failed),
            'elapsed_seconds': elapsed_time,
            'emails_per_second': rate,
            'failed_details': all_failed
        }
        
        logger.info(f"Email sending complete: {total_sent}/{len(emails)} sent successfully ({rate:.2f} emails/sec)")
        
        return results


# Example usage
if __name__ == "__main__":
    # Replace with your actual SparkPost API key
    API_KEY = "your-sparkpost-api-key-with-smtp-privileges"
    
    # Initialize the sender
    sender = SparkPostSMTPSender(
        api_key=API_KEY,
        from_email="verified-sender@yourdomain.com",
        max_connections=10,
        messages_per_connection=100
    )
    
    # Example batch of emails to send
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
        for i in range(1, 101)  # Generate 100 test emails
    ]
    
    # Send the emails
    results = sender.send_emails(test_emails, batch_size=25)
    
    # Print results summary
    print(f"Sent: {results['successfully_sent']}/{results['total_emails']} emails")
    print(f"Failed: {results['failed']} emails")
    print(f"Rate: {results['emails_per_second']:.2f} emails/second")
    
    # Print details of failed emails if any
    if results['failed'] > 0:
        print("\nFailed emails:")
        for i, failure in enumerate(results['failed_details'], 1):
            print(f"{i}. To: {failure['email_data']['to_email']} - Error: {failure['error']}")
