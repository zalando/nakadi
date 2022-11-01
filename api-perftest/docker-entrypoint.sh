#!/bin/bash
SIMULATION_NAME=${SIMULATION_NAME:-com.wiley.epdcs.eventhub.test.EventHubLoadTest}
java -cp bin/api-perftest-1.1.3-all.jar io.gatling.app.Gatling -nr -s ${SIMULATION_NAME}
aws s3 cp results s3://epdcs-nonprod-datamigration/test-lahiru-wijesinghe/simulationlogs --recursive
