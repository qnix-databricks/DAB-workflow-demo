# Databricks notebook source
from databricks.sdk import WorkspaceClient
from databricks.sdk.service import workspace

w = WorkspaceClient()

# COMMAND ----------

# Create a secret scope named: "foundry-migration"

w.secrets.create_scope("foundry-migration")

# COMMAND ----------

# Add a secret (key value pair) to the scope

w.secrets.put_secret(
  scope="foundry-migration", 
  key="api-token",
  string_value="<token-value-here>")

# COMMAND ----------

# Grant permission to the scope: who can read the secret.  <principal> should be a valid group name or user-id

w.secrets.put_acl(scope="foundry-migration",
                  principal="databricks-ps", 
                  permission=workspace.AclPermission.READ)

w.secrets.put_acl(scope="foundry-migration",
                  principal="charter-dev", 
                  permission=workspace.AclPermission.READ)