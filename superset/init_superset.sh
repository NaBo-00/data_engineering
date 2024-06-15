#!/bin/bash

# Function to create admin user
create_admin_user() {
  superset fab create-admin \
    --username "${ADMIN_USERNAME}" \
    --firstname "Superset" \
    --lastname "Admin" \
    --email "${ADMIN_EMAIL}" \
    --password "${ADMIN_PASSWORD}" || true
}

# Check if the admin user exists
admin_exists=$(superset fab list-users | grep -c "${ADMIN_USERNAME}")

# If the admin user does not exist, create it
if [ "$admin_exists" -eq "0" ]; then
  echo "Admin user does not exist. Creating admin user..."
  create_admin_user
else
  flask fab list-users
  echo "Admin user already exists."
fi

# Run the remaining initialization
echo "Run the remaining initialization..."
echo "-----------------------------------"
echo "Run superset db upgrade..."
superset db upgrade
echo "Run superset init..."
superset init
