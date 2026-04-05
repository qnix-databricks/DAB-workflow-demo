#!/bin/bash

# Databricks Asset Bundle Deployment Script
# This script builds the JAR and deploys it to Databricks using DABs

set -e

# Colors for output
RED='\033[0;31m'
GREEN='\033[0;32m'
YELLOW='\033[1;33m'
NC='\033[0m' # No Color

# Function to print colored messages
print_info() {
    echo -e "${GREEN}[INFO]${NC} $1"
}

print_warning() {
    echo -e "${YELLOW}[WARN]${NC} $1"
}

print_error() {
    echo -e "${RED}[ERROR]${NC} $1"
}

# Parse command line arguments
TARGET=${1:-dev}
ACTION=${2:-deploy}

print_info "Starting Databricks Asset Bundle deployment..."
print_info "Target environment: $TARGET"
print_info "Action: $ACTION"

# Check if Databricks CLI is installed
if ! command -v databricks &> /dev/null; then
    print_error "Databricks CLI is not installed!"
    print_info "Install it with: pip install databricks-cli"
    exit 1
fi

# Check if SBT is installed
if ! command -v sbt &> /dev/null; then
    print_error "SBT is not installed!"
    print_info "Install it from: https://www.scala-sbt.org/download.html"
    exit 1
fi

# Build the JAR
print_info "Building JAR with SBT..."
sbt clean assembly

# Check if JAR was built successfully
JAR_FILE=$(find target/scala-2.13 -name "workflow-demo-*.jar" | head -n 1)
if [ -z "$JAR_FILE" ]; then
    print_error "JAR file not found! Build may have failed."
    exit 1
fi

print_info "JAR built successfully: $JAR_FILE"

# Validate the bundle
print_info "Validating Databricks bundle..."
databricks bundle validate -t $TARGET

if [ $? -ne 0 ]; then
    print_error "Bundle validation failed!"
    exit 1
fi

print_info "Bundle validation successful!"

# Perform the requested action
case $ACTION in
    deploy)
        print_info "Deploying bundle to $TARGET environment..."
        databricks bundle deploy -t $TARGET
        print_info "✅ Deployment completed successfully!"
        print_info "View your jobs in the Databricks workspace under Workflows"
        ;;
    
    run)
        print_info "Deploying and running the job..."
        databricks bundle deploy -t $TARGET
        
        # Ask which job to run
        echo ""
        print_info "Available jobs:"
        echo "  1) simple_jar_job"
        echo "  2) example_jar_job"
        echo "  3) multi_task_workflow"
        echo ""
        read -p "Select job to run (1-3): " JOB_CHOICE
        
        case $JOB_CHOICE in
            1)
                JOB_KEY="simple_jar_job"
                ;;
            2)
                JOB_KEY="example_jar_job"
                ;;
            3)
                JOB_KEY="multi_task_workflow"
                ;;
            *)
                print_error "Invalid choice"
                exit 1
                ;;
        esac
        
        print_info "Running job: $JOB_KEY"
        databricks bundle run -t $TARGET $JOB_KEY
        ;;
    
    destroy)
        print_warning "This will destroy all resources in the $TARGET environment!"
        read -p "Are you sure? (yes/no): " CONFIRM
        if [ "$CONFIRM" = "yes" ]; then
            print_info "Destroying bundle resources..."
            databricks bundle destroy -t $TARGET
            print_info "✅ Resources destroyed"
        else
            print_info "Destruction cancelled"
        fi
        ;;
    
    *)
        print_error "Unknown action: $ACTION"
        print_info "Available actions: deploy, run, destroy"
        exit 1
        ;;
esac

print_info "Done!"

