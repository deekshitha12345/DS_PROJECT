#!/usr/bin/env python3
"""
Metrics Report Generator for Distributed Cache Benchmark

Processes raw metrics logs and generates CSV, JSON, and Markdown reports.
Can be used standalone or as a companion to the metrics_benchmark.sh script.

Usage:
    python scripts/metrics_processor.py --log artifacts/metrics/metrics_raw.log
    python scripts/metrics_processor.py --log metrics.log --output-dir reports
"""

import argparse
import json
import sys
from pathlib import Path
from datetime import datetime
from statistics import mean, median, stdev
from collections import defaultdict
from typing import Dict, List, Tuple, Any


class MetricsProcessor:
    """Process raw metrics logs and generate formatted reports."""
    
    def __init__(self, log_file: Path, output_dir: Path = None):
        self.log_file = log_file
        self.output_dir = output_dir or log_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.operations: List[Dict[str, Any]] = []
        self.parsed_successfully = False
        
    def parse_log(self) -> bool:
        """Parse raw metrics log file."""
        if not self.log_file.exists():
            print(f"Error: Log file not found: {self.log_file}")
            return False
        
        try:
            with open(self.log_file, 'r') as f:
                for line in f:
                    line = line.strip()
                    # Skip empty lines and comments
                    if not line or line.startswith('#'):
                        continue
                    
                    parts = line.split('|')
                    if len(parts) != 8:
                        continue
                    
                    operation = {
                        'timestamp': int(parts[0]),
                        'operation_type': parts[1],
                        'key': parts[2],
                        'elapsed_ms': int(parts[3]),
                        'http_code': int(parts[4]),
                        'status': parts[5],
                        'node_count': int(parts[6]),
                        'key_count': int(parts[7]),
                    }
                    self.operations.append(operation)
            
            self.parsed_successfully = True
            print(f"✓ Parsed {len(self.operations)} operations from {self.log_file}")
            return True
        
        except Exception as e:
            print(f"Error parsing log file: {e}")
            return False
    
    def calculate_percentile(self, values: List[float], percentile: float) -> float:
        """Calculate percentile value."""
        if not values:
            return 0.0
        sorted_values = sorted(values)
        index = int((percentile / 100.0) * len(sorted_values))
        index = min(index, len(sorted_values) - 1)
        return float(sorted_values[index])
    
    def get_stats(self, values: List[float]) -> Dict[str, float]:
        """Calculate statistics for a list of values."""
        if not values:
            return {
                'count': 0, 'mean': 0, 'median': 0,
                'min': 0, 'max': 0, 'p50': 0,
                'p95': 0, 'p99': 0, 'std_dev': 0
            }
        
        return {
            'count': len(values),
            'mean': mean(values),
            'median': median(values),
            'min': min(values),
            'max': max(values),
            'p50': self.calculate_percentile(values, 50),
            'p95': self.calculate_percentile(values, 95),
            'p99': self.calculate_percentile(values, 99),
            'std_dev': stdev(values) if len(values) > 1 else 0,
        }
    
    def generate_summary(self) -> Dict[str, Any]:
        """Generate summary statistics."""
        if not self.operations:
            return {}
        
        summary = {
            'total_operations': len(self.operations),
            'timestamp': datetime.now().isoformat(),
            'operations_by_type': {},
            'latency_stats': {},
            'by_node_count': {},
            'by_key_count': {},
            'success_rate': 0,
        }
        
        # Count successes
        successful = [op for op in self.operations if op['status'] == 'PASS']
        summary['success_rate'] = (len(successful) / len(self.operations)) * 100 if self.operations else 0
        
        # Stats by operation type
        for op_type in ['GET', 'PUT', 'DELETE']:
            ops_of_type = [op for op in self.operations if op['operation_type'] == op_type]
            if ops_of_type:
                latencies = [op['elapsed_ms'] for op in ops_of_type]
                successful_ops = [op for op in ops_of_type if op['status'] == 'PASS']
                
                summary['operations_by_type'][op_type] = {
                    'count': len(ops_of_type),
                    'successful': len(successful_ops),
                    'success_rate': (len(successful_ops) / len(ops_of_type)) * 100,
                    'latency': self.get_stats(latencies),
                    'throughput_ops_per_sec': len(ops_of_type) / (max([op['timestamp'] for op in ops_of_type]) - min([op['timestamp'] for op in ops_of_type]) + 1) if len(ops_of_type) > 1 else 0,
                }
        
        # Stats by node count
        by_node = defaultdict(list)
        for op in self.operations:
            by_node[op['node_count']].append(op)
        
        for node_count, ops in sorted(by_node.items()):
            latencies = [op['elapsed_ms'] for op in ops]
            summary['by_node_count'][node_count] = {
                'operations': len(ops),
                'latency': self.get_stats(latencies),
            }
        
        # Stats by key count
        by_keys = defaultdict(list)
        for op in self.operations:
            by_keys[op['key_count']].append(op)
        
        for key_count, ops in sorted(by_keys.items()):
            latencies = [op['elapsed_ms'] for op in ops]
            summary['by_key_count'][key_count] = {
                'operations': len(ops),
                'latency': self.get_stats(latencies),
            }
        
        return summary
    
    def generate_csv(self) -> bool:
        """Generate CSV report."""
        try:
            output_file = self.output_dir / 'metrics_results.csv'
            
            with open(output_file, 'w') as f:
                # Write header
                f.write('timestamp,operation_type,key,elapsed_ms,http_code,status,node_count,key_count\n')
                
                # Write data rows
                for op in self.operations:
                    f.write(f"{op['timestamp']},{op['operation_type']},{op['key']},"
                           f"{op['elapsed_ms']},{op['http_code']},{op['status']},"
                           f"{op['node_count']},{op['key_count']}\n")
            
            print(f"✓ CSV report: {output_file}")
            return True
        except Exception as e:
            print(f"Error generating CSV: {e}")
            return False
    
    def generate_json(self) -> bool:
        """Generate JSON report."""
        try:
            output_file = self.output_dir / 'metrics_results.json'
            summary = self.generate_summary()
            
            report = {
                'metadata': {
                    'generated_at': datetime.now().isoformat(),
                    'total_operations': len(self.operations),
                    'log_file': str(self.log_file),
                },
                'summary': summary,
                'operations': self.operations,
            }
            
            with open(output_file, 'w') as f:
                json.dump(report, f, indent=2)
            
            print(f"✓ JSON report: {output_file}")
            return True
        except Exception as e:
            print(f"Error generating JSON: {e}")
            return False
    
    def generate_markdown(self) -> bool:
        """Generate Markdown report."""
        try:
            output_file = self.output_dir / 'metrics_results.md'
            summary = self.generate_summary()
            
            with open(output_file, 'w') as f:
                f.write('# Distributed Cache Metrics Report\n\n')
                f.write(f'**Generated:** {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}\n\n')
                
                # Overall summary
                f.write('## Summary\n\n')
                f.write(f'- **Total Operations:** {summary.get("total_operations", 0)}\n')
                f.write(f'- **Success Rate:** {summary.get("success_rate", 0):.1f}%\n\n')
                
                # Operations breakdown
                if summary.get('operations_by_type'):
                    f.write('## Operations Breakdown\n\n')
                    f.write('| Operation Type | Count | Successful | Success Rate | Mean Latency (ms) | P95 Latency (ms) | P99 Latency (ms) | Throughput (ops/sec) |\n')
                    f.write('|---|---|---|---|---|---|---|---|\n')
                    
                    for op_type in ['GET', 'PUT', 'DELETE']:
                        op_stats = summary['operations_by_type'].get(op_type, {})
                        if op_stats:
                            latency = op_stats.get('latency', {})
                            f.write(f"| {op_type} | {op_stats.get('count', 0)} | {op_stats.get('successful', 0)} | "
                                   f"{op_stats.get('success_rate', 0):.1f}% | {latency.get('mean', 0):.2f} | "
                                   f"{latency.get('p95', 0):.2f} | {latency.get('p99', 0):.2f} | "
                                   f"{op_stats.get('throughput_ops_per_sec', 0):.2f} |\n")
                
                # Latency statistics by node count
                if summary.get('by_node_count'):
                    f.write('\n## Latency Statistics by Node Count\n\n')
                    f.write('| Node Count | Operations | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |\n')
                    f.write('|---|---|---|---|---|---|---|\n')
                    
                    for node_count in sorted(summary['by_node_count'].keys()):
                        stats = summary['by_node_count'][node_count]
                        latency = stats.get('latency', {})
                        f.write(f"| {node_count} | {stats.get('operations', 0)} | {latency.get('mean', 0):.2f} | "
                               f"{latency.get('min', 0):.2f} | {latency.get('max', 0):.2f} | "
                               f"{latency.get('p95', 0):.2f} | {latency.get('p99', 0):.2f} |\n")
                
                # Latency statistics by key count
                if summary.get('by_key_count'):
                    f.write('\n## Latency Statistics by Key Count\n\n')
                    f.write('| Key Count | Operations | Mean Latency (ms) | Min (ms) | Max (ms) | P95 (ms) | P99 (ms) |\n')
                    f.write('|---|---|---|---|---|---|---|\n')
                    
                    for key_count in sorted(summary['by_key_count'].keys()):
                        stats = summary['by_key_count'][key_count]
                        latency = stats.get('latency', {})
                        f.write(f"| {key_count} | {stats.get('operations', 0)} | {latency.get('mean', 0):.2f} | "
                               f"{latency.get('min', 0):.2f} | {latency.get('max', 0):.2f} | "
                               f"{latency.get('p95', 0):.2f} | {latency.get('p99', 0):.2f} |\n")
                
                # Detailed table (if not too many operations)
                if len(self.operations) <= 500:
                    f.write('\n## Detailed Operations Log\n\n')
                    f.write('| Timestamp | Operation | Latency (ms) | Status | Nodes | Keys |\n')
                    f.write('|---|---|---|---|---|---|\n')
                    
                    for op in self.operations:
                        ts = datetime.fromtimestamp(op['timestamp']).strftime('%H:%M:%S')
                        f.write(f"| {ts} | {op['operation_type']} | {op['elapsed_ms']} | {op['status']} | "
                               f"{op['node_count']} | {op['key_count']} |\n")
            
            print(f"✓ Markdown report: {output_file}")
            return True
        except Exception as e:
            print(f"Error generating Markdown: {e}")
            return False
    
    def process(self, formats: List[str] = None) -> bool:
        """Process metrics and generate reports."""
        if formats is None:
            formats = ['csv', 'json', 'md']
        
        if not self.parse_log():
            return False
        
        success = True
        for fmt in formats:
            if fmt.lower() == 'csv':
                success = self.generate_csv() and success
            elif fmt.lower() == 'json':
                success = self.generate_json() and success
            elif fmt.lower() == 'md':
                success = self.generate_markdown() and success
        
        return success


def main():
    parser = argparse.ArgumentParser(
        description='Process distributed cache metrics logs and generate reports'
    )
    parser.add_argument(
        '--log',
        type=Path,
        default=Path('artifacts/metrics/metrics_raw.log'),
        help='Path to raw metrics log file'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Output directory for reports (defaults to log file directory)'
    )
    parser.add_argument(
        '--format',
        type=str,
        default='csv,json,md',
        help='Output formats: csv, json, md (comma-separated, default: all)'
    )
    
    args = parser.parse_args()
    formats = [f.strip().lower() for f in args.format.split(',')]
    
    processor = MetricsProcessor(args.log, args.output_dir)
    
    if processor.process(formats):
        print("\n✓ All reports generated successfully!")
        return 0
    else:
        print("\n✗ Some reports failed to generate")
        return 1


if __name__ == '__main__':
    sys.exit(main())
