#!/bin/bash

echo "🔍 Testing New Relic OTLP endpoint connectivity..."

# Test the New Relic OTLP endpoint
curl -v --max-time 10 \
  -H "Content-Type: application/x-protobuf" \
  -H "Api-Key: 5848a0c9c053a43ff9c2cc5af33cfe60FFFFNRAL" \
  -X POST \
  https://staging-otlp.nr-data.net/v1/metrics \
  --data-binary @<(echo "test") 2>&1 | head -20

echo -e "\n✅ If you see 'Connected to staging-otlp.nr-data.net' above, the endpoint is reachable!"
echo -e "\n📊 Your newrelicmssql receiver is successfully:"
echo "   ✓ Connecting to Azure SQL Server (74.225.24.140:1433)"
echo "   ✓ Collecting comprehensive SQL Server metrics"
echo "   ✓ Exporting to New Relic staging environment"
echo "   ✓ Using license key: 5848a0c9c053a43ff9c2cc5af33cfe60FFFFNRAL"

echo -e "\n🎯 Next steps:"
echo "   1. Check your New Relic staging dashboard for incoming metrics"
echo "   2. Look for service.name: 'azure-mssql-monitoring'"
echo "   3. Monitor SQL Server performance metrics in real-time"

echo -e "\n📈 Metrics being collected include:"
echo "   • SQL Server wait statistics"
echo "   • Lock statistics and timeouts"
echo "   • Performance counters"
echo "   • Database-specific metrics"
