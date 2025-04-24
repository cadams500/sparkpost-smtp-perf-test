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
        # Initialize timing statistics
        self.timing_stats = {
            'connection_setup_total': [],
            'connection_initial': [],
            'connection_ehlo': [],
            'connection_tls': [],
            'connection_post_tls_ehlo': [],
            'authentication': [],
            'message_creation': [],
            'message_send': [],
            'message_smtp_commands': {
                'MAIL': [],
                'RCPT': [],
                'DATA': [],
                'MESSAGE': [],
                'QUIT': []
            },
            'connection_refresh': [],
            'connection_cleanup': []
        }

    def _update_timing_stats(self, state: str, duration_ms: float):
        """Update timing statistics for a given state."""
        self.timing_stats[state].append(duration_ms)

    def _get_timing_summary(self) -> Dict[str, Dict[str, float]]:
        """Get summary statistics for all timing states."""
        summary = {}
        for state, times in self.timing_stats.items():
            if isinstance(times, dict):
                # Handle SMTP command timings
                cmd_summary = {}
                for cmd, cmd_times in times.items():
                    if cmd_times:
                        cmd_summary[cmd] = {
                            'min': float(min(cmd_times)),
                            'avg': float(sum(cmd_times)) / len(cmd_times),
                            'max': float(max(cmd_times)),
                            'total': float(sum(cmd_times)),
                            'count': len(cmd_times)
                        }
                summary[state] = cmd_summary
            elif times:
                # Handle regular timing stats
                summary[state] = {
                    'min': float(min(times)),
                    'avg': float(sum(times)) / len(times),
                    'max': float(max(times)),
                    'total': float(sum(times)),
                    'count': len(times)
                }
        return summary

    def _log_smtp_command_timing(self, command: str, start_time: float):
        """Log timing for an SMTP command."""
        duration = (time.time() - start_time) * 1000  # Convert to milliseconds
        if command in self.timing_stats['message_smtp_commands']:
            self.timing_stats['message_smtp_commands'][command].append(duration)
        else:
            logger.warning(f"Unknown SMTP command: {command}")

    def _log_timing_waterfall(self, timing_summary: Dict[str, Dict[str, float]], total_messages: int):
        """Log timing statistics in a hierarchical waterfall format."""
        logger.info("\nTiming Waterfall (ms):")
        
        # Helper function to safely get timing values
        def get_timing(key: str, stat: str = 'avg') -> float:
            if key in timing_summary and stat in timing_summary[key]:
                return timing_summary[key][stat]
            return 0.0
        
        # Connection setup phase
        setup_avg = get_timing('connection_setup_total')
        logger.info(f"┌─ Connection Setup (avg: {setup_avg:.2f}ms)")
        logger.info(f"│  ├─ Initial Connection: {get_timing('connection_initial'):.2f}ms")
        logger.info(f"│  ├─ EHLO: {get_timing('connection_ehlo'):.2f}ms")
        logger.info(f"│  ├─ TLS: {get_timing('connection_tls'):.2f}ms")
        logger.info(f"│  ├─ Post-TLS EHLO: {get_timing('connection_post_tls_ehlo'):.2f}ms")
        logger.info(f"│  └─ Authentication: {get_timing('authentication'):.2f}ms")
        
        # Message processing phase
        msg_send_avg = get_timing('message_send')
        msg_create_avg = get_timing('message_creation')
        logger.info(f"├─ Message Processing (per message)")
        logger.info(f"│  ├─ Creation: {msg_create_avg:.2f}ms")
        logger.info(f"│  └─ SMTP Commands:")
        
        # Handle SMTP command timings
        if 'message_smtp_commands' in timing_summary:
            for cmd, times in timing_summary['message_smtp_commands'].items():
                if times['count'] > 0:
                    logger.info(f"│     ├─ {cmd}: {times['avg']:.2f}ms")
        logger.info(f"│     └─ Total Send: {msg_send_avg:.2f}ms")
        
        # Connection cleanup
        cleanup_avg = get_timing('connection_cleanup')
        logger.info(f"└─ Connection Cleanup: {cleanup_avg:.2f}ms")
        
        # Calculate timing metrics
        total_time = sum(stats['total'] for stats in timing_summary.values() if 'total' in stats)
        setup_count = get_timing('connection_setup_total', 'count')
        messages_per_connection = total_messages / setup_count if setup_count > 0 else 0
        
        # Connection overhead per message
        connection_overhead = (setup_avg + cleanup_avg) / messages_per_connection if messages_per_connection > 0 else 0
        
        # Calculate rates
        messages_per_second = total_messages / (total_time/1000) if total_time > 0 else 0
        messages_per_hour = messages_per_second * 3600
        
        logger.info(f"\nTiming Metrics:")
        logger.info(f"Total elapsed time: {total_time/1000:.2f}s")
        logger.info(f"Messages sent: {total_messages}")
        logger.info(f"Messages per connection: {messages_per_connection:.1f}")
        logger.info(f"Connection overhead per message: {connection_overhead:.2f}ms")
        logger.info(f"Message processing time: {msg_send_avg:.2f}ms")
        logger.info(f"Total time per message: {(connection_overhead + msg_send_avg):.2f}ms")
        logger.info(f"Rate: {messages_per_second:.2f} messages/second")
        logger.info(f"Rate: {messages_per_hour:,.0f} messages/hour")

    def create_smtp_connection(self) -> smtplib.SMTP:
        """Create and return an authenticated SMTP connection with pipelining enabled."""
        # Track total connection setup time
        setup_start = time.time()
        
        # Create SMTP connection with timeout
        start_time = time.time()
        smtp = smtplib.SMTP(self.host, self.port, timeout=30)  # 30 second timeout
        self._update_timing_stats('connection_initial', (time.time() - start_time) * 1000)
        
        try:
            # Identify ourselves to the SMTP server
            start_time = time.time()
            smtp.ehlo_or_helo_if_needed()
            self._update_timing_stats('connection_ehlo', (time.time() - start_time) * 1000)
            
            # Check if pipelining is supported
            has_pipelining = smtp.has_extn('pipelining')
            if has_pipelining:
                logger.debug("SMTP server supports pipelining")
            else:
                logger.warning("SMTP server does not support pipelining")
                
            # Start TLS if requested
            if self.use_tls:
                logger.debug("SMTP using TLS")
                start_time = time.time()
                context = ssl.create_default_context()
                smtp.starttls(context=context)
                self._update_timing_stats('connection_tls', (time.time() - start_time) * 1000)
                
                start_time = time.time()
                smtp.ehlo()  # Need to re-identify after TLS
                self._update_timing_stats('connection_post_tls_ehlo', (time.time() - start_time) * 1000)
                
            # Authenticate with SparkPost SMTP credentials
            auth_start = time.time()
            smtp.login('SMTP_Injection', self.api_key)
            self._update_timing_stats('authentication', (time.time() - auth_start) * 1000)
            
            # Record total connection setup time
            self._update_timing_stats('connection_setup_total', (time.time() - setup_start) * 1000)
            
            return smtp
        except Exception as e:
            # Ensure connection is closed if there's an error
            try:
                smtp.quit()
            except:
                pass
            raise e
        
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
        message_latencies = []  # Track latency for each message
        smtp = None
        
        try:
            # Create a new connection for this batch
            smtp = self.create_smtp_connection()
            
            for i, email_data in enumerate(batch):
                try:
                    # Validate email_data is a dictionary
                    if not isinstance(email_data, dict):
                        raise ValueError(f"Expected dictionary for email data, got {type(email_data)}")
                    
                    # Validate required fields
                    required_fields = ['to_email', 'subject', 'text_content']
                    missing_fields = [field for field in required_fields if field not in email_data]
                    if missing_fields:
                        raise ValueError(f"Missing required fields: {', '.join(missing_fields)}")
                    
                    # Create the message
                    msg_start = time.time()
                    msg = self.create_message(
                        to_email=email_data['to_email'],
                        subject=email_data['subject'],
                        text_content=email_data['text_content'],
                        html_content=email_data.get('html_content'),
                        custom_headers=email_data.get('custom_headers')
                    )
                    self._update_timing_stats('message_creation', (time.time() - msg_start) * 1000)
                    
                    # Track start time for this message
                    send_start = time.time()
                    
                    # Send the message and track individual SMTP command timings
                    mail_start = time.time()
                    code, resp = smtp.mail(self.from_email)
                    self._log_smtp_command_timing('MAIL', mail_start)
                    if code != 250:
                        raise Exception(f"MAIL FROM failed: {resp}")
                    
                    rcpt_start = time.time()
                    code, resp = smtp.rcpt(email_data['to_email'])
                    self._log_smtp_command_timing('RCPT', rcpt_start)
                    if code != 250:
                        raise Exception(f"RCPT TO failed: {resp}")
                    
                    # Send the message content
                    msg_data_start = time.time()
                    code, resp = smtp.data(msg.as_string())
                    self._log_smtp_command_timing('MESSAGE', msg_data_start)
                    if code != 250:
                        raise Exception(f"DATA failed: {resp}")
                    
                    # Calculate total message send time
                    send_time = (time.time() - send_start) * 1000
                    message_latencies.append(send_time)
                    self._update_timing_stats('message_send', send_time)
                    sent_count += 1
                    
                    # Refresh connection after messages_per_connection
                    if (i + 1) % self.messages_per_connection == 0 and i + 1 < len(batch):
                        refresh_start = time.time()
                        quit_start = time.time()
                        code, resp = smtp.quit()
                        self._log_smtp_command_timing('QUIT', quit_start)
                        if code != 221:
                            logger.warning(f"QUIT failed: {resp}")
                        smtp = self.create_smtp_connection()
                        self._update_timing_stats('connection_refresh', (time.time() - refresh_start) * 1000)
                        
                except Exception as e:
                    logger.error(f"Failed to send message to {email_data.get('to_email', 'unknown')}: {str(e)}")
                    logger.error("Full exception:", exc_info=True)
                    failed_messages.append({
                        'email_data': email_data,
                        'error': str(e)
                    })
                    # If we have a connection error, try to create a new connection
                    if smtp is not None:
                        try:
                            quit_start = time.time()
                            code, resp = smtp.quit()
                            self._log_smtp_command_timing('QUIT', quit_start)
                            if code != 221:
                                logger.warning(f"QUIT failed: {resp}")
                        except:
                            pass
                        reconnect_start = time.time()
                        smtp = self.create_smtp_connection()
                        self._update_timing_stats('connection_refresh', (time.time() - reconnect_start) * 1000)
                    
        except Exception as e:
            logger.error(f"Batch sending error: {str(e)}")
            logger.debug(f"Batch error details: {str(e)}", exc_info=True)
            # Mark all remaining messages in batch as failed
            for email_data in batch[sent_count:]:
                failed_messages.append({
                    'email_data': email_data,
                    'error': f"Batch error: {str(e)}"
                })
        finally:
            # Ensure connection is always closed
            if smtp is not None:
                try:
                    quit_start = time.time()
                    code, resp = smtp.quit()
                    self._log_smtp_command_timing('QUIT', quit_start)
                    self._update_timing_stats('connection_cleanup', (time.time() - quit_start) * 1000)
                except:
                    pass
                
        return sent_count, failed_messages, message_latencies
        
    def send_emails(self, emails: List[Dict[str, Any]], batch_size: int = 50) -> Dict[str, Any]:
        """Send emails using multiple concurrent connections for optimal throughput."""
        start_time = time.time()
        total_sent = 0
        all_failed = []
        all_latencies = []  # Collect latencies from all batches
        
        # Split emails into batches
        batches = [emails[i:i + batch_size] for i in range(0, len(emails), batch_size)]
        num_connections = min(self.max_connections, len(batches))
        logger.info(f"Sending {len(emails)} emails in {len(batches)} batches using up to {num_connections} concurrent connections")
        
        # Use a thread pool to send batches concurrently
        with concurrent.futures.ThreadPoolExecutor(max_workers=num_connections) as executor:
            futures = [executor.submit(self.send_batch, batch) for batch in batches]
            
            for future in concurrent.futures.as_completed(futures):
                sent, failed, latencies = future.result()
                total_sent += sent
                all_failed.extend(failed)
                all_latencies.extend(latencies)
                
        elapsed_time = time.time() - start_time
        rate = total_sent / elapsed_time if elapsed_time > 0 else 0
        
        # Calculate average latency in milliseconds
        avg_latency = sum(all_latencies) / len(all_latencies) if all_latencies else 0
        
        # Calculate implied latency (total time / number of messages)
        implied_latency = (elapsed_time * 1000) / total_sent if total_sent > 0 else 0
        
        # Get timing summary
        timing_summary = self._get_timing_summary()
        
        # Log timing waterfall
        self._log_timing_waterfall(timing_summary, total_sent)
        
        results = {
            'total_emails': len(emails),
            'successfully_sent': total_sent,
            'failed': len(all_failed),
            'elapsed_seconds': elapsed_time,
            'emails_per_second': rate,
            'avg_latency_ms': avg_latency,
            'min_latency_ms': min(all_latencies) if all_latencies else 0,
            'max_latency_ms': max(all_latencies) if all_latencies else 0,
            'implied_latency_ms': implied_latency,
            'timing_stats': timing_summary,
            'failed_details': all_failed
        }
        
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
