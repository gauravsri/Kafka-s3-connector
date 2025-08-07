#!/bin/bash

# Git repository initialization script for Kafka S3 Connector
# Creates comprehensive .gitignore and initial commit

set -e

BASE_DIR="/Users/gaurav/kafka-s3-connector"

echo "=========================================================================="
echo "                    GIT REPOSITORY INITIALIZATION"
echo "=========================================================================="
echo "Initializing git repository for Kafka S3 Connector"
echo "Project: Enterprise Kafka Connect S3 Delta Lake Connector"
echo "=========================================================================="

cd "$BASE_DIR"

# Function to create .gitignore
create_gitignore() {
    echo "📝 Creating comprehensive .gitignore file..."
    
    cat > .gitignore << 'EOF'
# Kafka S3 Connector - Git Ignore Rules

# Maven
target/
pom.xml.tag
pom.xml.releaseBackup
pom.xml.versionsBackup
pom.xml.next
release.properties
dependency-reduced-pom.xml
buildNumber.properties
.mvn/timing.properties
.mvn/wrapper/maven-wrapper.jar

# Java
*.class
*.log
*.ctxt
.mtj.tmp/
*.jar
*.war
*.nar
*.ear
*.zip
*.tar.gz
*.rar
hs_err_pid*
replay_pid*

# IDE
.idea/
*.iws
*.iml
*.ipr
.vscode/
*.swp
*.swo
*~

# Eclipse
.apt_generated
.classpath
.factorypath
.project
.settings
.springBeans
.sts4-cache

# NetBeans
/nbproject/private/
/nbbuild/
/dist/
/nbdist/
/.nb-gradle/
build/

# OS
.DS_Store
.DS_Store?
._*
.Spotlight-V100
.Trashes
ehthumbs.db
Thumbs.db

# Kafka Connect Runtime
connect.offsets
/tmp/
/logs/
*.log
worker*.log
connect-configs
connect-offsets
connect-status

# Local Development
/data/
/test-data/
.env
.env.local
.env.*.local

# Secrets and Configuration
*.key
*.pem
*.p12
*.jks
application-*.properties
!application-local.yml
!application-prod.yml

# Test Results
/test-results/
/coverage/
*.exec

# Temporary Files
tmp/
temp/
.tmp/
.temp/

# Container Data
/minio-data/
/kafka-data/
/redpanda-data/

# Build and Deployment
/dist/
/deploy/
kubernetes-local/
docker-compose.override.yml

# Monitoring and Metrics
/prometheus-data/
/grafana-data/

# Documentation Build
/docs/_build/
/docs/site/

# Local Scripts
*-local.sh
test-local-*.sh

# Keep Important Files
!.gitignore
!README.md
!CONTRIBUTING.md
!LICENSE
!impl_steps.md
EOF
    
    echo "✅ .gitignore created with comprehensive rules"
}

# Function to initialize git repository
init_git() {
    echo "🔧 Initializing git repository..."
    
    if [ -d ".git" ]; then
        echo "⚠️  Git repository already exists"
        echo "Current git status:"
        git status --porcelain | head -10
    else
        git init
        echo "✅ Git repository initialized"
    fi
    
    # Configure git for better Java development
    git config core.autocrlf input
    git config core.filemode true
    git config merge.tool vimdiff
    git config diff.tool vimdiff
    
    echo "✅ Git configuration applied"
}

# Function to check git configuration
check_git_config() {
    echo "📋 Checking git configuration..."
    
    if git config --global user.name > /dev/null; then
        echo "✅ Git user.name: $(git config --global user.name)"
    else
        echo "⚠️  Git user.name not set globally"
        echo "Run: git config --global user.name 'Your Name'"
    fi
    
    if git config --global user.email > /dev/null; then
        echo "✅ Git user.email: $(git config --global user.email)"
    else
        echo "⚠️  Git user.email not set globally"
        echo "Run: git config --global user.email 'your.email@example.com'"
    fi
}

# Function to stage files
stage_files() {
    echo "📦 Staging files for initial commit..."
    
    # Add source code
    git add src/
    echo "  ✅ Source code staged"
    
    # Add configuration
    git add config/
    git add src/main/resources/
    echo "  ✅ Configuration files staged"
    
    # Add scripts
    git add scripts/
    chmod +x scripts/*.sh
    echo "  ✅ Scripts staged and made executable"
    
    # Add Maven build file
    git add pom.xml
    echo "  ✅ Maven configuration staged"
    
    # Add documentation
    git add README.md impl_steps.md 2>/dev/null || true
    echo "  ✅ Documentation staged"
    
    # Add git configuration
    git add .gitignore
    echo "  ✅ Git configuration staged"
    
    # Show what's staged
    echo
    echo "Files staged for commit:"
    git diff --cached --name-only | head -20
    
    STAGED_COUNT=$(git diff --cached --name-only | wc -l)
    echo "Total files staged: $STAGED_COUNT"
    
    if [ "$STAGED_COUNT" -gt 20 ]; then
        echo "... and $((STAGED_COUNT - 20)) more files"
    fi
}

# Function to create initial commit
create_initial_commit() {
    echo "💾 Creating initial commit..."
    
    # Check if there are staged changes
    if git diff --cached --quiet; then
        echo "⚠️  No staged changes to commit"
        return
    fi
    
    # Create comprehensive commit message
    COMMIT_MESSAGE=$(cat << 'EOF'
Initial implementation: Enterprise Kafka Connect S3 Delta Lake Connector

🚀 Features Implemented:
- Kafka Connect API with distributed mode support
- Horizontal scaling with multiple workers
- Enterprise patterns (SOLID, Factory, Strategy, Builder)
- Comprehensive metrics and monitoring
- Structured logging with correlation IDs
- Circuit breaker and retry mechanisms
- Dead Letter Queue (DLQ) functionality
- S3 integration with MinIO support
- Multi-topic configuration system
- Time-based and default partitioning strategies

🏗️ Architecture:
- S3DeltaSinkConnector: Main connector implementation
- S3DeltaSinkTask: Task processing with record handling
- Factory Pattern: WriterFactory, PartitionStrategyFactory
- Strategy Pattern: Multiple partitioning strategies
- Builder Pattern: S3DeltaWriter, ConnectorMetricsSnapshot
- AOP: Performance logging and monitoring

🧪 Testing:
- Local integration tests with Podman containers
- Horizontal scaling test scripts
- Data verification and integrity checks
- Load generation for performance testing
- Comprehensive environment verification

📊 Monitoring & Observability:
- Micrometer metrics with Prometheus integration
- Custom actuator endpoints for connector monitoring
- Performance logging with configurable thresholds
- Real-time health checks and status reporting

🔧 Development Environment:
- Local development with RedPanda and MinIO containers
- Distributed Kafka Connect worker configuration
- Automated setup and verification scripts
- Comprehensive documentation and implementation guide

Ready for production deployment and horizontal scaling testing.

🤖 Generated with Enterprise Java Patterns and Best Practices
EOF
)
    
    git commit -m "$COMMIT_MESSAGE"
    
    echo "✅ Initial commit created successfully"
    
    # Show commit info
    echo
    echo "Commit Details:"
    git log --oneline -1
    git show --stat HEAD
}

# Function to create tags
create_tags() {
    echo "🏷️  Creating version tags..."
    
    # Create initial version tag
    git tag -a "v1.0.0" -m "Version 1.0.0 - Enterprise Kafka Connect S3 Connector

Features:
- Horizontal scaling support
- Enterprise architecture patterns
- Comprehensive monitoring
- Production-ready configuration
- Local development environment
"
    
    # Create development milestone tag
    git tag -a "milestone-horizontal-scaling" -m "Milestone: Horizontal Scaling Implementation

Achievements:
- 2-worker distributed mode testing
- Task distribution and failover
- Data integrity verification  
- Performance monitoring
- Load testing with 10K+ messages
"
    
    echo "✅ Tags created:"
    git tag -l
}

# Function to show repository summary
show_summary() {
    echo
    echo "=========================================================================="
    echo "                    GIT REPOSITORY SUMMARY"
    echo "=========================================================================="
    
    echo "📊 Repository Statistics:"
    echo "  Total commits: $(git rev-list --count HEAD)"
    echo "  Total files tracked: $(git ls-files | wc -l)"
    echo "  Repository size: $(du -sh .git | cut -f1)"
    
    echo
    echo "📁 File Structure:"
    git ls-files | head -20
    
    TOTAL_FILES=$(git ls-files | wc -l)
    if [ "$TOTAL_FILES" -gt 20 ]; then
        echo "  ... and $((TOTAL_FILES - 20)) more files"
    fi
    
    echo
    echo "🏷️  Tags:"
    git tag -l
    
    echo
    echo "📋 Next Steps:"
    echo "  1. Review staged files: git status"
    echo "  2. View commit history: git log --oneline"
    echo "  3. Create remote repository and push:"
    echo "     git remote add origin <repository-url>"
    echo "     git push -u origin main"
    echo "     git push --tags"
    echo
    echo "🚀 Ready for development and collaboration!"
    echo "=========================================================================="
}

# Main execution
main() {
    check_git_config
    echo
    
    create_gitignore
    echo
    
    init_git
    echo
    
    stage_files
    echo
    
    create_initial_commit
    echo
    
    create_tags
    echo
    
    show_summary
}

# Run main function
main "$@"