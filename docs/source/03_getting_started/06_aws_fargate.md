# Running the containers on AWS ECS Fargate

Setting up and maintaning the Kubernetes cluster just to run the ML pipelines seems like too much overhead for a simple ML projects. Some cloud providers offer an ability to run the containers in "serverless" manner, in AWS the service is called [AWS Fargate](https://aws.amazon.com/fargate/).

The plugin can integrate with AWS Fargate as the execution engine, as a replacement for Kubernetes. For this case, [AWS Managed Workflows for Apache Airflow](https://aws.amazon.com/managed-workflows-for-apache-airflow/) is a great alternative to setting up Apache Airflow manually.

## Task Definition registration

AWS Fargate doesn't allow to run a container with any arbitary image. Instead, the specification is registered as Task Definition first and then you start Task Definitions on Fargate.

In order to create a Task Definition to run the pipeline, you can either use the UI wizard, or the following terraform script:

```
resource "aws_ecs_task_definition" "service" {
  family = "<name-of-your-pipeline>"

  requires_compatibilities = ["FARGATE"]
  network_mode             = "awsvpc"
  cpu                      = 1024
  memory                   = 2048
  execution_role_arn       = module.ecs_execution_role.arn
  task_role_arn            = module.ecs_task_role.arn
  container_definitions    = <<DEFINITION
    [
      {
        "name": "main",
        "image": "to-be-overriden-by-ci-cd-put-any-text-here",
        "essential": true,
        "logConfiguration": {
          "logDriver": "awslogs",
          "options": {
            "awslogs-group": "${aws_cloudwatch_log_group.log_group.name}",
            "awslogs-region": "eu-central-1",
            "awslogs-stream-prefix": "ecs"
          }
        }
      }
    ]
  DEFINITION

  runtime_platform {
    operating_system_family = "LINUX"
    cpu_architecture        = "X86_64"
  }
}
```

Also, updating an image requires creating a new version of Task Definition that would point to it. You can use the following lines in your CI/CD code to achieve this, keeping the other properties of Task Definition intact ([source](https://github.com/aws/aws-cli/issues/3064#issuecomment-638751296)):

```
export ECR_IMAGE="..."
NEW_DEF=`aws ecs describe-task-definition --task-definition <name-of-your-pipeline> --query '{  containerDefinitions: taskDefinition.containerDefinitions,
          family: taskDefinition.family,
          taskRoleArn: taskDefinition.taskRoleArn,
          executionRoleArn: taskDefinition.executionRoleArn,
          networkMode: taskDefinition.networkMode,
          volumes: taskDefinition.volumes,
          placementConstraints: taskDefinition.placementConstraints,
          requiresCompatibilities: taskDefinition.requiresCompatibilities,
          cpu: taskDefinition.cpu,
          memory: taskDefinition.memory}' | jq --arg IMAGE "$ECR_IMAGE" '.containerDefinitions[0].image = $IMAGE'`
aws ecs register-task-definition --cli-input-json "$NEW_DEF"
```

## Adjusting the configuration file

AWS Fargate requires a bit of network configuration details in order to run your containers. In order to set these, add the following section in the plugin configuration file:

```
# Type of execution engine to use.
execution_engine:
  type: aws-fargate
  cluster: <name-of-your-ECS-cluster>
  task_definition: <name-of-your-pipeline>
  subnets:
  - <private-subnet-the-container-should-start-in>
  security_groups:
  - <some-limited-security-group>
```

## Unsupported features

AWS Fargate doesn't allow to create volumes for temporary data storage (like Kubernetes does). Therefore, all the paths in your `catalog.yaml` should point to S3 or any other storage and configuration section for volume should be disabled:

```
run_config:
  volume:
    disabled: True
```
