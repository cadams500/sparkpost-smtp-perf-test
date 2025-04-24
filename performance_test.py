import os
import time
import psutil
import logging
import numpy as np
import matplotlib.pyplot as plt
from dotenv import load_dotenv
from smtp import SparkPostSMTPSender
from typing import List, Dict, Any
import concurrent.futures
from datetime import datetime

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('performance_test')

class SystemMonitor:
    def __init__(self):
        self.cpu_samples = []
        self.memory_samples = []
        self.network_samples = []
        self.start_time = time.time()
        
    def sample(self):
        """Take a sample of system metrics."""
        self.cpu_samples.append(psutil.cpu_percent())
        self.memory_samples.append(psutil.virtual_memory().percent)
        net_io = psutil.net_io_counters()
        self.network_samples.append({
            'bytes_sent': net_io.bytes_sent,
            'bytes_recv': net_io.bytes_recv
        })
        
    def get_averages(self) -> Dict[str, float]:
        """Calculate average system metrics."""
        return {
            'cpu_avg': np.mean(self.cpu_samples),
            'memory_avg': np.mean(self.memory_samples),
            'network_bytes_sent': self.network_samples[-1]['bytes_sent'] - self.network_samples[0]['bytes_sent'],
            'network_bytes_recv': self.network_samples[-1]['bytes_recv'] - self.network_samples[0]['bytes_recv'],
            'duration': time.time() - self.start_time
        }

def run_test(
    sender: SparkPostSMTPSender,
    test_emails: List[Dict[str, Any]],
    max_connections: int,
    messages_per_connection: int,
    batch_size: int
) -> Dict[str, Any]:
    """Run a single test with specific configuration."""
    monitor = SystemMonitor()
    
    # Update sender configuration
    sender.max_connections = max_connections
    sender.messages_per_connection = messages_per_connection
    
    # Start monitoring
    monitor.sample()
    
    # Run the test
    results = sender.send_emails(test_emails, batch_size=batch_size)
    
    # Final monitoring sample
    monitor.sample()
    
    # Get system metrics
    system_metrics = monitor.get_averages()
    
    # Combine results
    return {
        **results,
        'system_metrics': system_metrics,
        'config': {
            'max_connections': max_connections,
            'messages_per_connection': messages_per_connection,
            'batch_size': batch_size
        }
    }

def plot_results(results: List[Dict[str, Any]], output_dir: str = 'performance_results'):
    """Generate plots from test results."""
    os.makedirs(output_dir, exist_ok=True)
    timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
    
    # Create subplots
    fig, ((ax1, ax2), (ax3, ax4)) = plt.subplots(2, 2, figsize=(15, 10))
    
    # Group results by max_connections for better visualization
    results_by_conn = {}
    for result in results:
        max_conn = result['config']['max_connections']
        if max_conn not in results_by_conn:
            results_by_conn[max_conn] = []
        results_by_conn[max_conn].append(result)
    
    # Plot 1: Rate vs Connections
    max_conns = sorted(results_by_conn.keys())
    rates = [np.mean([r['emails_per_second'] for r in results_by_conn[conn]]) for conn in max_conns]
    ax1.plot(max_conns, rates, 'o-')
    ax1.set_xlabel('Max Connections')
    ax1.set_ylabel('Emails per Second')
    ax1.set_title('Rate vs Connections')
    ax1.grid(True)
    
    # Plot 2: Latency vs Connections
    avg_latencies = [np.mean([r['avg_latency_ms'] for r in results_by_conn[conn]]) for conn in max_conns]
    ax2.plot(max_conns, avg_latencies, 'o-')
    ax2.set_xlabel('Max Connections')
    ax2.set_ylabel('Average Latency (ms)')
    ax2.set_title('Latency vs Connections')
    ax2.grid(True)
    
    # Plot 3: System Usage
    cpu_usage = [np.mean([r['system_metrics']['cpu_avg'] for r in results_by_conn[conn]]) for conn in max_conns]
    memory_usage = [np.mean([r['system_metrics']['memory_avg'] for r in results_by_conn[conn]]) for conn in max_conns]
    ax3.plot(max_conns, cpu_usage, 'o-', label='CPU %')
    ax3.plot(max_conns, memory_usage, 'o-', label='Memory %')
    ax3.set_xlabel('Max Connections')
    ax3.set_ylabel('Usage %')
    ax3.set_title('System Resource Usage')
    ax3.legend()
    ax3.grid(True)
    
    # Plot 4: Rate vs Messages per Connection (grouped by max_connections)
    msgs_per_conn_range = sorted(set(r['config']['messages_per_connection'] for r in results))
    for max_conn in max_conns:
        conn_results = results_by_conn[max_conn]
        rates = []
        for msgs in msgs_per_conn_range:
            matching_results = [r for r in conn_results if r['config']['messages_per_connection'] == msgs]
            if matching_results:
                rates.append(np.mean([r['emails_per_second'] for r in matching_results]))
            else:
                rates.append(None)
        ax4.plot(msgs_per_conn_range, rates, 'o-', label=f'{max_conn} conns')
    
    ax4.set_xlabel('Messages per Connection')
    ax4.set_ylabel('Emails per Second')
    ax4.set_title('Rate vs Messages per Connection')
    ax4.legend(title='Max Connections')
    ax4.grid(True)
    
    plt.tight_layout()
    plt.savefig(f'{output_dir}/performance_{timestamp}.png')
    plt.close()

def find_optimal_config(results: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Find the optimal configuration based on performance metrics."""
    # Score each configuration based on rate and latency
    for result in results:
        # Higher rate is better, lower latency is better
        rate_score = result['emails_per_second']
        latency_score = 1 / (result['avg_latency_ms'] / 1000)  # Convert to seconds for scoring
        system_score = 1 / (result['system_metrics']['cpu_avg'] / 100)  # Lower CPU usage is better
        
        # Combined score (weighted)
        result['score'] = (rate_score * 0.5) + (latency_score * 0.3) + (system_score * 0.2)
    
    # Sort by score and return best configuration
    best_result = max(results, key=lambda x: x['score'])
    return best_result

def main():
    # Load environment variables
    load_dotenv()
    
    # Get SparkPost API key from environment
    api_key = os.getenv('SPARKPOST_API_KEY')
    if not api_key:
        raise ValueError("SPARKPOST_API_KEY environment variable not set")
    
    # Get sender email from environment or use default
    from_email = os.getenv('FROM_EMAIL', 'your-verified-sender@yourdomain.com')
    
    # Test configurations to try
    max_connections_range = [10, 20, 30, 40, 50, 60]
    messages_per_connection_range = [50, 100, 150, 200, 500]
    batch_size = 5  # Keep batch size constant for this test
    
    # Create test emails
    test_emails = [
        {
            "to_email": f"recipient{i}@example.com.sink.sparkpostmail.com",
            "subject": f"Test Email {i}",
            "text_content": f"This is test email {i} plain text content",
            "html_content": f"<html><body><h1>Test Email {i}</h1><p>This is the HTML content.</p></body></html>",
            "custom_headers": {
                "X-Campaign-ID": "sparkpost-performance-test-campaign"
            }
        }
        for i in range(1, 501)
    ]
    
    # Initialize sender
    sender = SparkPostSMTPSender(
        api_key=api_key,
        from_email=from_email,
        max_connections=10,  # Initial value, will be updated in tests
        messages_per_connection=100
    )
    
    # Run tests
    results = []
    for max_conn in max_connections_range:
        for msgs_per_conn in messages_per_connection_range:
            logger.info(f"Testing configuration: max_connections={max_conn}, messages_per_connection={msgs_per_conn}")
            result = run_test(sender, test_emails, max_conn, msgs_per_conn, batch_size)
            results.append(result)
            
            # Log results
            logger.info(f"Results for max_connections={max_conn}, messages_per_connection={msgs_per_conn}:")
            logger.info(f"  Rate: {result['emails_per_second']:.2f} emails/second")
            logger.info(f"  Rate: {result['emails_per_second'] * 3600:,.0f} emails/hour")
            logger.info(f"  Latency (ms): min={result['min_latency_ms']:.2f}, avg={result['avg_latency_ms']:.2f}, max={result['max_latency_ms']:.2f}")
            logger.info(f"  CPU Usage: {result['system_metrics']['cpu_avg']:.1f}%")
            logger.info(f"  Memory Usage: {result['system_metrics']['memory_avg']:.1f}%")
    
    # Generate plots
    plot_results(results)
    
    # Find optimal configuration
    optimal = find_optimal_config(results)
    logger.info("\nOptimal Configuration:")
    logger.info(f"Max Connections: {optimal['config']['max_connections']}")
    logger.info(f"Messages per Connection: {optimal['config']['messages_per_connection']}")
    logger.info(f"Rate: {optimal['emails_per_second']:.2f} emails/second")
    logger.info(f"Rate: {optimal['emails_per_second'] * 3600:,.0f} emails/hour")
    logger.info(f"Average Latency: {optimal['avg_latency_ms']:.2f}ms")
    logger.info(f"CPU Usage: {optimal['system_metrics']['cpu_avg']:.1f}%")
    logger.info(f"Memory Usage: {optimal['system_metrics']['memory_avg']:.1f}%")

if __name__ == "__main__":
    main() 