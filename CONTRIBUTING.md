# Contributing to Kafka-S3-Delta Lake Connector

Thank you for your interest in contributing to the Kafka-S3-Delta Lake Connector! This guide will help you get started with contributing to the project.

## 📋 Table of Contents

1. [Getting Started](#getting-started)
2. [Development Setup](#development-setup)
3. [Code Style](#code-style)
4. [Testing Guidelines](#testing-guidelines)
5. [Submitting Changes](#submitting-changes)
6. [Issue Guidelines](#issue-guidelines)
7. [Documentation](#documentation)
8. [Community](#community)

## Getting Started

### Prerequisites

Before contributing, ensure you have:

- **Java 17+** installed and configured
- **Maven 3.9+** for dependency management
- **Docker & Docker Compose** for local testing
- **Git** with your GitHub account configured
- **IDE** with Java support (IntelliJ IDEA, VS Code, Eclipse)

### Fork and Clone

1. Fork the repository on GitHub
2. Clone your fork locally:

```bash
git clone https://github.com/YOUR_USERNAME/Kafka-s3-connector.git
cd Kafka-s3-connector
```

3. Add the upstream repository:

```bash
git remote add upstream https://github.com/gauravsri/Kafka-s3-connector.git
```

4. Create a feature branch:

```bash
git checkout -b feature/your-feature-name
```

## Development Setup

### Local Environment

1. **Start infrastructure**:

```bash
# Start Kafka (RedPanda) and MinIO
docker-compose up -d

# Verify services
docker-compose ps
```

2. **Build the project**:

```bash
mvn clean compile
```

3. **Run tests**:

```bash
mvn test
```

4. **Start the application**:

```bash
mvn spring-boot:run
```

### IDE Configuration

#### IntelliJ IDEA

1. Import as Maven project
2. Enable annotation processing
3. Install plugins: Lombok, Spring Boot
4. Set JDK to Java 17+

#### VS Code

1. Install extensions: Extension Pack for Java, Spring Boot Extension Pack
2. Open project folder
3. Trust workspace
4. Configure Java home in settings

### Project Structure

```
src/
├── main/
│   ├── java/com/company/kafkaconnector/
│   │   ├── config/              # Configuration classes
│   │   ├── service/             # Business logic services
│   │   ├── model/               # Data models and DTOs
│   │   ├── exception/           # Custom exceptions
│   │   └── utils/               # Utility classes
│   └── resources/
│       ├── application.yml      # Application configuration
│       ├── schemas/             # JSON schemas
│       └── logback-spring.xml   # Logging configuration
└── test/
    ├── java/                    # Unit and integration tests
    └── resources/               # Test resources
```

## Code Style

### Java Code Style

We follow Google Java Style with some modifications:

1. **Indentation**: 4 spaces (not tabs)
2. **Line length**: 120 characters maximum
3. **Naming**: 
   - Classes: PascalCase
   - Methods/Variables: camelCase
   - Constants: UPPER_SNAKE_CASE
   - Packages: lowercase

### Code Quality

#### Static Analysis

Run static analysis before committing:

```bash
# Checkstyle
mvn checkstyle:check

# SpotBugs
mvn spotbugs:check

# PMD
mvn pmd:check
```

#### Code Examples

**Good Example**:

```java
@Service
public class DeltaWriterService {
    private static final Logger logger = LoggerFactory.getLogger(DeltaWriterService.class);
    private static final int DEFAULT_BATCH_SIZE = 1000;
    
    private final DeltaKernelWriterService deltaKernelWriter;
    private final MetricsService metricsService;
    
    public DeltaWriterService(DeltaKernelWriterService deltaKernelWriter,
                             MetricsService metricsService) {
        this.deltaKernelWriter = deltaKernelWriter;
        this.metricsService = metricsService;
    }
    
    @Transactional
    public ProcessingResult writeMessage(String topic, Map<String, Object> message) {
        Validate.notNull(topic, "Topic cannot be null");
        Validate.notNull(message, "Message cannot be null");
        
        try {
            return processMessageInternal(topic, message);
        } catch (Exception e) {
            logger.error("Failed to write message for topic: {}", topic, e);
            metricsService.incrementError(topic);
            throw new DeltaWriteException("Failed to write message", e);
        }
    }
}
```

**Bad Example**:

```java
@Service
public class deltawriterservice {
    Logger log = LoggerFactory.getLogger(deltawriterservice.class);
    int BATCHSIZE = 1000;
    
    DeltaKernelWriterService writer;
    
    public void writeMsg(String t, Map m) {
        // No validation
        // No proper error handling
        // Poor naming
        writer.write(t, m);
    }
}
```

### Documentation

#### JavaDoc Standards

```java
/**
 * Writes a message to Delta Lake table with ACID guarantees.
 * 
 * <p>This method performs the following operations:
 * <ul>
 *   <li>Validates the input message against the configured schema</li>
 *   <li>Accumulates the message in a batch for efficient processing</li>
 *   <li>Flushes the batch when size or time limits are reached</li>
 * </ul>
 * 
 * @param topic the Kafka topic name, must not be null
 * @param message the message to write, must not be null
 * @return the processing result with metadata
 * @throws DeltaWriteException if the write operation fails
 * @throws ValidationException if the message fails schema validation
 * @since 1.0.0
 */
@Transactional
public ProcessingResult writeMessage(String topic, Map<String, Object> message) {
    // Implementation
}
```

## Testing Guidelines

### Test Structure

Follow the AAA pattern (Arrange, Act, Assert):

```java
@Test
void shouldWriteMessageToDeltaLake() {
    // Arrange
    String topic = "test.topic";
    Map<String, Object> message = createTestMessage();
    when(deltaKernelWriter.write(any(), any())).thenReturn(true);
    
    // Act
    ProcessingResult result = deltaWriterService.writeMessage(topic, message);
    
    // Assert
    assertThat(result.isSuccess()).isTrue();
    assertThat(result.getRecordsProcessed()).isEqualTo(1);
    verify(deltaKernelWriter).write(eq(topic), eq(message));
}
```

### Test Categories

#### Unit Tests

Test individual components in isolation:

```java
@ExtendWith(MockitoExtension.class)
class SchemaValidationServiceTest {
    
    @Mock
    private JsonSchemaValidator validator;
    
    @InjectMocks
    private SchemaValidationService schemaValidationService;
    
    @Test
    void shouldValidateMessageSuccessfully() {
        // Test implementation
    }
}
```

#### Integration Tests

Test component interactions:

```java
@SpringBootTest
@TestPropertySource(properties = {
    "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}",
    "aws.s3.endpoint=http://localhost:9000"
})
class DeltaWriterIntegrationTest {
    
    @Autowired
    private DeltaWriterService deltaWriterService;
    
    @Test
    void shouldWriteToActualDeltaTable() {
        // Integration test implementation
    }
}
```

#### End-to-End Tests

Test complete workflows:

```bash
# Run E2E tests
mvn test -Dtest="*E2ETest"
```

### Test Coverage

Maintain minimum 80% test coverage:

```bash
# Generate coverage report
mvn jacoco:report

# View report
open target/site/jacoco/index.html
```

## Submitting Changes

### Commit Guidelines

We follow [Conventional Commits](https://www.conventionalcommits.org/):

#### Commit Format

```
<type>[optional scope]: <description>

[optional body]

[optional footer(s)]
```

#### Types

- `feat`: New feature
- `fix`: Bug fix
- `docs`: Documentation changes
- `style`: Code style changes (formatting, etc.)
- `refactor`: Code refactoring
- `perf`: Performance improvements
- `test`: Adding or updating tests
- `chore`: Maintenance tasks

#### Examples

```bash
feat(delta): add support for nested object schemas
fix(kafka): resolve consumer group rebalancing issue
docs(readme): update setup instructions
test(integration): add Delta Lake write verification
```

### Pull Request Process

1. **Update your branch**:

```bash
git fetch upstream
git rebase upstream/main
```

2. **Run all checks**:

```bash
# Run tests
mvn clean test

# Run static analysis
mvn checkstyle:check spotbugs:check pmd:check

# Check formatting
mvn fmt:check
```

3. **Create pull request**:
   - Use descriptive title and description
   - Link related issues
   - Add screenshots for UI changes
   - Mark as draft if work in progress

4. **PR Template**:

```markdown
## Description
Brief description of changes

## Type of Change
- [ ] Bug fix
- [ ] New feature
- [ ] Breaking change
- [ ] Documentation update

## Testing
- [ ] Unit tests pass
- [ ] Integration tests pass
- [ ] Manual testing completed

## Checklist
- [ ] Code follows style guidelines
- [ ] Self-review completed
- [ ] Documentation updated
- [ ] Tests added/updated
```

### Review Process

1. **Automated checks**: All CI checks must pass
2. **Code review**: At least one maintainer review
3. **Testing**: Verify tests cover new/changed functionality
4. **Documentation**: Ensure docs are updated if needed

## Issue Guidelines

### Reporting Bugs

Use the bug report template:

```markdown
## Bug Description
Clear description of the bug

## Steps to Reproduce
1. Step one
2. Step two
3. Step three

## Expected Behavior
What you expected to happen

## Actual Behavior
What actually happened

## Environment
- Java version:
- Operating system:
- Kafka version:
- S3 storage:

## Additional Context
Any additional information
```

### Feature Requests

Use the feature request template:

```markdown
## Feature Description
Clear description of the feature

## Use Case
Why is this feature needed?

## Proposed Solution
How should this feature work?

## Alternatives
What alternatives have you considered?

## Additional Context
Any additional information
```

### Issue Labels

- `bug`: Something isn't working
- `enhancement`: New feature or request
- `documentation`: Documentation improvements
- `good first issue`: Good for newcomers
- `help wanted`: Extra attention needed
- `performance`: Performance related
- `security`: Security related

## Documentation

### Documentation Types

1. **Code Documentation**: JavaDoc comments
2. **README**: Project overview and quick start
3. **User Guide**: Detailed usage instructions
4. **Design Docs**: Architectural decisions
5. **API Docs**: Generated API documentation

### Writing Guidelines

- **Clear and concise**: Use simple, direct language
- **Examples**: Include code examples and use cases
- **Structure**: Use headings, lists, and formatting
- **Links**: Link to related sections and resources
- **Updates**: Keep documentation current with code changes

### Documentation Workflow

```bash
# Generate API docs
mvn javadoc:javadoc

# Build site documentation
mvn site

# Preview documentation
open target/site/index.html
```

## Community

### Communication Channels

- **GitHub Issues**: Bug reports and feature requests
- **GitHub Discussions**: General questions and discussions
- **Pull Requests**: Code review and collaboration

### Code of Conduct

We follow the [Contributor Covenant Code of Conduct](https://www.contributor-covenant.org/version/2/1/code_of_conduct/). Please read and follow these guidelines to ensure a welcoming environment for all contributors.

### Getting Help

- **Documentation**: Check README, User Guide, and Design docs
- **Issues**: Search existing issues before creating new ones
- **Discussions**: Use GitHub Discussions for questions
- **Code Review**: Ask for help in pull request comments

## Recognition

Contributors are recognized in:

- **Contributors section** in README.md
- **Release notes** for significant contributions
- **Hall of Fame** for major contributions

Thank you for contributing to the Kafka-S3-Delta Lake Connector! 🚀