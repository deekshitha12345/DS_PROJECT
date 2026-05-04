#!/usr/bin/env python3
"""
Metrics Visualization Script

Generates plots from metrics data to visualize:
- Latency trends by key count
- Latency trends by node count
- Throughput comparisons
- Success rates

Requirements: matplotlib, numpy, pandas

Install: pip install matplotlib numpy pandas
"""

import argparse
import json
import sys
from pathlib import Path
from collections import defaultdict
from statistics import mean

# Try to import plotting libraries
try:
    import matplotlib.pyplot as plt
    import matplotlib.patches as mpatches
    from matplotlib.gridspec import GridSpec
    import numpy as np
except ImportError:
    print("Error: matplotlib and numpy are required for plotting")
    print("Install with: pip install matplotlib numpy")
    sys.exit(1)


class MetricsVisualizer:
    """Generate plots from metrics data."""
    
    def __init__(self, json_file: Path, output_dir: Path = None):
        self.json_file = json_file
        self.output_dir = output_dir or json_file.parent
        self.output_dir.mkdir(parents=True, exist_ok=True)
        
        self.data = None
        self.operations = []
        
    def load_data(self) -> bool:
        """Load metrics from JSON file."""
        if not self.json_file.exists():
            print(f"Error: File not found: {self.json_file}")
            return False
        
        try:
            with open(self.json_file, 'r') as f:
                self.data = json.load(f)
            self.operations = self.data.get('operations', [])
            print(f"✓ Loaded {len(self.operations)} operations from {self.json_file}")
            return True
        except Exception as e:
            print(f"Error loading JSON: {e}")
            return False
    
    def plot_latency_by_key_count(self) -> bool:
        """Plot latency variation by key count."""
        try:
            # Group operations by key count and operation type
            by_keys = defaultdict(lambda: defaultdict(list))
            
            for op in self.operations:
                if op['status'] == 'PASS':
                    key_count = op['key_count']
                    op_type = op['operation_type']
                    latency = op['elapsed_ms']
                    by_keys[key_count][op_type].append(latency)
            
            if not by_keys:
                print("No data for latency by key count plot")
                return False
            
            # Prepare data for plotting
            key_counts = sorted(by_keys.keys())
            op_types = sorted(set().union(*[set(by_keys[kc].keys()) for kc in key_counts]))
            
            fig, axes = plt.subplots(1, len(op_types), figsize=(15, 5))
            if len(op_types) == 1:
                axes = [axes]
            
            colors = {'GET': '#2563eb', 'PUT': '#dc2626', 'DELETE': '#16a34a'}
            
            for idx, op_type in enumerate(op_types):
                ax = axes[idx]
                latencies = []
                min_latencies = []
                max_latencies = []
                
                for kc in key_counts:
                    if op_type in by_keys[kc]:
                        lats = by_keys[kc][op_type]
                        latencies.append(mean(lats))
                        min_latencies.append(min(lats))
                        max_latencies.append(max(lats))
                    else:
                        latencies.append(0)
                        min_latencies.append(0)
                        max_latencies.append(0)
                
                # Plot with error bars
                errors = [
                    [latencies[i] - min_latencies[i] for i in range(len(latencies))],
                    [max_latencies[i] - latencies[i] for i in range(len(latencies))]
                ]
                
                ax.bar(range(len(key_counts)), latencies, 
                      color=colors.get(op_type, '#gray'), alpha=0.7, edgecolor='black')
                ax.errorbar(range(len(key_counts)), latencies, 
                           yerr=errors, fmt='none', ecolor='black', capsize=5)
                
                ax.set_xlabel('Key Count', fontsize=11, fontweight='bold')
                ax.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
                ax.set_title(f'{op_type} Operations - Key Count Impact', fontsize=12, fontweight='bold')
                ax.set_xticks(range(len(key_counts)))
                ax.set_xticklabels([f'{kc:,}' for kc in key_counts])
                ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            plt.tight_layout()
            output_file = self.output_dir / 'latency_by_key_count.png'
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"✓ Generated: {output_file}")
            plt.close()
            return True
        
        except Exception as e:
            print(f"Error generating latency by key count plot: {e}")
            return False
    
    def plot_latency_by_node_count(self) -> bool:
        """Plot latency variation by node count."""
        try:
            # Group operations by node count and operation type
            by_nodes = defaultdict(lambda: defaultdict(list))
            
            for op in self.operations:
                if op['status'] == 'PASS':
                    node_count = op['node_count']
                    op_type = op['operation_type']
                    latency = op['elapsed_ms']
                    by_nodes[node_count][op_type].append(latency)
            
            if not by_nodes:
                print("No data for latency by node count plot")
                return False
            
            # Prepare data for plotting
            node_counts = sorted(by_nodes.keys())
            op_types = sorted(set().union(*[set(by_nodes[nc].keys()) for nc in node_counts]))
            
            fig, axes = plt.subplots(1, len(op_types), figsize=(15, 5))
            if len(op_types) == 1:
                axes = [axes]
            
            colors = {'GET': '#2563eb', 'PUT': '#dc2626', 'DELETE': '#16a34a'}
            
            for idx, op_type in enumerate(op_types):
                ax = axes[idx]
                latencies = []
                min_latencies = []
                max_latencies = []
                
                for nc in node_counts:
                    if op_type in by_nodes[nc]:
                        lats = by_nodes[nc][op_type]
                        latencies.append(mean(lats))
                        min_latencies.append(min(lats))
                        max_latencies.append(max(lats))
                    else:
                        latencies.append(0)
                        min_latencies.append(0)
                        max_latencies.append(0)
                
                # Plot with error bars
                errors = [
                    [latencies[i] - min_latencies[i] for i in range(len(latencies))],
                    [max_latencies[i] - latencies[i] for i in range(len(latencies))]
                ]
                
                ax.bar(range(len(node_counts)), latencies,
                      color=colors.get(op_type, '#gray'), alpha=0.7, edgecolor='black')
                ax.errorbar(range(len(node_counts)), latencies,
                           yerr=errors, fmt='none', ecolor='black', capsize=5)
                
                ax.set_xlabel('Node Count', fontsize=11, fontweight='bold')
                ax.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
                ax.set_title(f'{op_type} Operations - Node Scaling Impact', fontsize=12, fontweight='bold')
                ax.set_xticks(range(len(node_counts)))
                ax.set_xticklabels([f'{nc}' for nc in node_counts])
                ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            plt.tight_layout()
            output_file = self.output_dir / 'latency_by_node_count.png'
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"✓ Generated: {output_file}")
            plt.close()
            return True
        
        except Exception as e:
            print(f"Error generating latency by node count plot: {e}")
            return False
    
    def plot_percentile_comparison(self) -> bool:
        """Plot P50, P95, P99 percentiles for different configurations."""
        try:
            summary = self.data.get('summary', {})
            
            if not summary.get('by_node_count'):
                print("No node count data for percentile plot")
                return False
            
            # Prepare data
            node_counts = sorted([int(k) for k in summary['by_node_count'].keys()])
            
            p50_vals = []
            p95_vals = []
            p99_vals = []
            
            for nc in node_counts:
                latency_stats = summary['by_node_count'][str(nc)].get('latency', {})
                p50_vals.append(latency_stats.get('p50', 0))
                p95_vals.append(latency_stats.get('p95', 0))
                p99_vals.append(latency_stats.get('p99', 0))
            
            # Create plot
            fig, ax = plt.subplots(figsize=(10, 6))
            
            x = np.arange(len(node_counts))
            width = 0.25
            
            ax.bar(x - width, p50_vals, width, label='P50 (Median)', color='#3b82f6', alpha=0.8)
            ax.bar(x, p95_vals, width, label='P95 (95th %ile)', color='#f59e0b', alpha=0.8)
            ax.bar(x + width, p99_vals, width, label='P99 (99th %ile)', color='#ef4444', alpha=0.8)
            
            ax.set_xlabel('Node Count', fontsize=12, fontweight='bold')
            ax.set_ylabel('Latency (ms)', fontsize=12, fontweight='bold')
            ax.set_title('Latency Percentiles by Node Count', fontsize=14, fontweight='bold')
            ax.set_xticks(x)
            ax.set_xticklabels([f'{nc}' for nc in node_counts])
            ax.legend(fontsize=11)
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            plt.tight_layout()
            output_file = self.output_dir / 'latency_percentiles.png'
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"✓ Generated: {output_file}")
            plt.close()
            return True
        
        except Exception as e:
            print(f"Error generating percentile plot: {e}")
            return False
    
    def plot_operation_comparison(self) -> bool:
        """Plot GET vs PUT vs DELETE comparison."""
        try:
            summary = self.data.get('summary', {})
            ops_by_type = summary.get('operations_by_type', {})
            
            if not ops_by_type:
                print("No operation type data for comparison plot")
                return False
            
            op_types = sorted(ops_by_type.keys())
            
            fig, axes = plt.subplots(2, 2, figsize=(14, 10))
            fig.suptitle('Operation Type Comparison', fontsize=16, fontweight='bold')
            
            colors_map = {'GET': '#2563eb', 'PUT': '#dc2626', 'DELETE': '#16a34a'}
            colors = [colors_map.get(op, '#gray') for op in op_types]
            
            # Count
            ax = axes[0, 0]
            counts = [ops_by_type[op]['count'] for op in op_types]
            ax.bar(op_types, counts, color=colors, alpha=0.7, edgecolor='black')
            ax.set_ylabel('Count', fontsize=11, fontweight='bold')
            ax.set_title('Operation Count', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            # Mean Latency
            ax = axes[0, 1]
            mean_lats = [ops_by_type[op]['latency']['mean'] for op in op_types]
            ax.bar(op_types, mean_lats, color=colors, alpha=0.7, edgecolor='black')
            ax.set_ylabel('Latency (ms)', fontsize=11, fontweight='bold')
            ax.set_title('Mean Latency', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            # Success Rate
            ax = axes[1, 0]
            success_rates = [ops_by_type[op]['success_rate'] for op in op_types]
            ax.bar(op_types, success_rates, color=colors, alpha=0.7, edgecolor='black')
            ax.set_ylabel('Success Rate (%)', fontsize=11, fontweight='bold')
            ax.set_title('Success Rate', fontsize=12, fontweight='bold')
            ax.set_ylim(0, 105)
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            # P99 Latency
            ax = axes[1, 1]
            p99_lats = [ops_by_type[op]['latency']['p99'] for op in op_types]
            ax.bar(op_types, p99_lats, color=colors, alpha=0.7, edgecolor='black')
            ax.set_ylabel('P99 Latency (ms)', fontsize=11, fontweight='bold')
            ax.set_title('P99 Latency (Worst Case)', fontsize=12, fontweight='bold')
            ax.grid(axis='y', alpha=0.3, linestyle='--')
            
            plt.tight_layout()
            output_file = self.output_dir / 'operation_comparison.png'
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"✓ Generated: {output_file}")
            plt.close()
            return True
        
        except Exception as e:
            print(f"Error generating operation comparison plot: {e}")
            return False
    
    def plot_heatmap(self) -> bool:
        """Plot heatmap of latency by node count and key count."""
        try:
            # Create matrix of latencies
            key_counts = sorted(set(op['key_count'] for op in self.operations))
            node_counts = sorted(set(op['node_count'] for op in self.operations))
            
            if not key_counts or not node_counts:
                print("Insufficient data for heatmap")
                return False
            
            # Initialize matrix
            matrix = np.zeros((len(key_counts), len(node_counts)))
            counts = np.zeros((len(key_counts), len(node_counts)))
            
            # Fill matrix with average latencies
            for op in self.operations:
                if op['status'] == 'PASS' and op['operation_type'] == 'GET':
                    kc_idx = key_counts.index(op['key_count'])
                    nc_idx = node_counts.index(op['node_count'])
                    matrix[kc_idx, nc_idx] += op['elapsed_ms']
                    counts[kc_idx, nc_idx] += 1
            
            # Calculate averages
            with np.errstate(divide='ignore', invalid='ignore'):
                matrix = matrix / counts
                matrix[~np.isfinite(matrix)] = 0
            
            # Create heatmap
            fig, ax = plt.subplots(figsize=(10, 6))
            
            im = ax.imshow(matrix, cmap='RdYlGn_r', aspect='auto')
            
            ax.set_xticks(range(len(node_counts)))
            ax.set_yticks(range(len(key_counts)))
            ax.set_xticklabels([f'{nc} node{"s" if nc > 1 else ""}' for nc in node_counts])
            ax.set_yticklabels([f'{kc:,} keys' for kc in key_counts])
            
            ax.set_xlabel('Node Count', fontsize=12, fontweight='bold')
            ax.set_ylabel('Key Count', fontsize=12, fontweight='bold')
            ax.set_title('GET Latency Heatmap (ms)', fontsize=14, fontweight='bold')
            
            # Add colorbar
            cbar = plt.colorbar(im, ax=ax)
            cbar.set_label('Latency (ms)', fontsize=11, fontweight='bold')
            
            # Add text annotations
            for i in range(len(key_counts)):
                for j in range(len(node_counts)):
                    if counts[i, j] > 0:
                        text = ax.text(j, i, f'{matrix[i, j]:.0f}',
                                     ha="center", va="center", color="black", fontweight='bold')
            
            plt.tight_layout()
            output_file = self.output_dir / 'latency_heatmap.png'
            plt.savefig(output_file, dpi=150, bbox_inches='tight')
            print(f"✓ Generated: {output_file}")
            plt.close()
            return True
        
        except Exception as e:
            print(f"Error generating heatmap: {e}")
            return False
    
    def generate_all(self) -> bool:
        """Generate all plots."""
        if not self.load_data():
            return False
        
        print("\nGenerating plots...")
        all_success = True
        
        all_success = self.plot_latency_by_key_count() and all_success
        all_success = self.plot_latency_by_node_count() and all_success
        all_success = self.plot_percentile_comparison() and all_success
        all_success = self.plot_operation_comparison() and all_success
        all_success = self.plot_heatmap() and all_success
        
        return all_success


def main():
    parser = argparse.ArgumentParser(
        description='Generate visualizations from distributed cache metrics'
    )
    parser.add_argument(
        '--json',
        type=Path,
        default=Path('artifacts/metrics/metrics_results.json'),
        help='Path to metrics JSON file'
    )
    parser.add_argument(
        '--output-dir',
        type=Path,
        default=None,
        help='Output directory for plots (defaults to JSON directory)'
    )
    
    args = parser.parse_args()
    
    visualizer = MetricsVisualizer(args.json, args.output_dir)
    
    if visualizer.generate_all():
        print(f"\n✓ All plots generated successfully in: {visualizer.output_dir}")
        return 0
    else:
        print("\n✗ Some plots failed to generate")
        return 1


if __name__ == '__main__':
    sys.exit(main())
