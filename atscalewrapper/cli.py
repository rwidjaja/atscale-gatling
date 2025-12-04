"""CLI wrapper for the project.

Provides `create_cli_parser()` and `run_cli_mode(args)` to match
what the top-level `main.py` expects.
"""
import argparse
from .core import AtScaleGatlingCore


def create_cli_parser():
    """Return an ArgumentParser that parses CLI-specific options.

    This parser purposely excludes a `--mode` option because the
    top-level launcher handles that.
    """
    parser = argparse.ArgumentParser(add_help=False)
    parser.add_argument('--executor', help='Executor to run (required for CLI mode)')
    parser.add_argument('--models', help='Comma-separated list of models to run')
    parser.add_argument('--all-models', action='store_true', help='Run with all discovered models')
    parser.add_argument('--follow', action='store_true', help='Follow logs in real-time')
    return parser


def run_cli_mode(args):
    """Create core and run the underlying CLI implementation."""
    core = AtScaleGatlingCore()
    return _run_cli_mode(args, core)
"""CLI mode functionality"""
import argparse
import sys
from atscalewrapper.core import AtScaleGatlingCore

def run_cli_mode(args):
    """Run in CLI mode with command line arguments"""
    core = AtScaleGatlingCore()
    
    print("🚀 AtScale Gatling CLI Mode")
    print("=" * 50)
    
    # Discover models
    print("Discovering catalogs and cubes...")
    if not core.discover_and_setup():
        print("❌ Failed to discover models")
        return 1
    
    if not core.catalog_cube_pairs:
        print("❌ No catalog/cube pairs found")
        return 1
    
    # Handle model selection
    selected_models = []
    
    if args.models:
        # Use specified models
        specified_models = args.models.split(',')
        for model in specified_models:
            model = model.strip()
            # Try to find matching catalog::cube pair
            found = False
            for pair in core.catalog_cube_pairs:
                if model in pair:
                    selected_models.append(pair)
                    found = True
                    break
            if not found:
                print(f"❌ Model '{model}' not found in discovered pairs")
                return 1
    elif args.all_models:
        # Use all models
        selected_models = core.catalog_cube_pairs
    else:
        # Interactive selection
        print("\nAvailable catalog/cube pairs:")
        for i, pair in enumerate(core.catalog_cube_pairs, 1):
            print(f"  {i}. {pair}")
        
        try:
            selection = input(f"\nSelect models (comma-separated numbers 1-{len(core.catalog_cube_pairs)}, or 'all'): ").strip()
            if selection.lower() == 'all':
                selected_models = core.catalog_cube_pairs
            else:
                indices = [int(x.strip()) - 1 for x in selection.split(',')]
                selected_models = [core.catalog_cube_pairs[i] for i in indices if 0 <= i < len(core.catalog_cube_pairs)]
        except (ValueError, IndexError):
            print("❌ Invalid selection")
            return 1
    
    if not selected_models:
        print("❌ No models selected")
        return 1
    
    print(f"\n✅ Selected {len(selected_models)} models:")
    for model in selected_models:
        print(f"  - {model}")
    
    # Handle executor selection
    if args.executor:
        executor = args.executor
        if executor not in core.executors:
            print(f"❌ Executor '{executor}' not found")
            print(f"Available executors: {', '.join(core.executors)}")
            return 1
    else:
        # Interactive executor selection
        print("\nAvailable executors:")
        for i, executor in enumerate(core.executors, 1):
            print(f"  {i}. {executor}")
        
        try:
            selection = int(input(f"\nSelect executor (1-{len(core.executors)}): ")) - 1
            if 0 <= selection < len(core.executors):
                executor = core.executors[selection]
            else:
                print("❌ Invalid executor selection")
                return 1
        except ValueError:
            print("❌ Invalid selection")
            return 1
    
    print(f"\n🎯 Starting simulation with:")
    print(f"  Executor: {executor}")
    print(f"  Models: {len(selected_models)} selected")
    print(f"  Follow logs: {args.follow}")
    print("=" * 50)
    
    # Run the simulation
    try:
        success = core.run_executor(executor, selected_models, follow_logs=args.follow)
        return 0 if success else 1
    except KeyboardInterrupt:
        print("\n⏹️  Simulation interrupted by user")
        return 1
    except Exception as e:
        print(f"❌ Error: {e}")
        return 1

def create_cli_parser():
    """Create CLI argument parser"""
    parser = argparse.ArgumentParser(description='AtScale Gatling Controller - CLI Mode')
    
    # CLI-specific arguments
    parser.add_argument('--executor', help='Executor to run (required for CLI mode)')
    parser.add_argument('--models', help='Comma-separated list of models to run')
    parser.add_argument('--all-models', action='store_true', help='Run with all discovered models')
    parser.add_argument('--follow', action='store_true', help='Follow logs in real-time')
    
    return parser