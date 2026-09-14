param(
    [string]$BaseUrl = "http://localhost:8080",
    [string]$Username = "admin",
    [string]$Password = "local-dev-password",
    [switch]$SkipHealthCheck
)

$ErrorActionPreference = "Stop"
$headers = @{}
$credential = [Convert]::ToBase64String([Text.Encoding]::ASCII.GetBytes("${Username}:${Password}"))
$headers.Authorization = "Basic $credential"
$applicantId = "11111111-1111-4111-8111-111111111111"
$merchantId = "demo-merchant-001"

function Invoke-DemoRequest {
    param(
        [string]$Method,
        [string]$Uri,
        [object]$Body
    )

    $request = @{
        Method = $Method
        Uri = "$BaseUrl$Uri"
        Headers = $headers
        ContentType = "application/json"
    }
    if ($null -ne $Body) {
        $request.Body = ($Body | ConvertTo-Json -Depth 8)
    }
    Invoke-RestMethod @request
}

Write-Host "EventDrivenMicroservices demo"
Write-Host "Target: $BaseUrl"

if (-not $SkipHealthCheck) {
    Write-Host "`n[1/5] Checking application health..."
    Invoke-DemoRequest -Method Get -Uri "/actuator/health" | ConvertTo-Json -Depth 8
}

Write-Host "`n[2/5] Submitting a loan application..."
$loan = Invoke-DemoRequest -Method Post -Uri "/api/v1/loans" -Body @{
    applicantId = $applicantId
    amount = 25000.00
    termMonths = 36
}
$loan | ConvertTo-Json -Depth 8

Write-Host "`n[3/5] Sending a normal transaction..."
Invoke-DemoRequest -Method Post -Uri "/api/v1/transactions" -Body @{
    transactionId = "demo-normal-001"
    amount = 100.00
    currency = "USD"
    merchantId = $merchantId
    correlationId = "demo-normal-001"
    settlementStatus = "PENDING"
    timestamp = (Get-Date).ToUniversalTime().ToString("o")
} | ConvertTo-Json -Depth 8

Write-Host "`n[4/5] Sending a velocity burst..."
1..6 | ForEach-Object {
    Invoke-DemoRequest -Method Post -Uri "/api/v1/transactions" -Body @{
        transactionId = "demo-burst-$($_)"
        amount = 2000.00
        currency = "USD"
        merchantId = $merchantId
        correlationId = "demo-burst-$($_)"
        settlementStatus = "PENDING"
        timestamp = (Get-Date).ToUniversalTime().ToString("o")
    } | Out-Null
}
Write-Host "Submitted six transactions for $merchantId."

Write-Host "`n[5/5] Reading outbox and ledger evidence..."
Write-Host "Outbox status:"
Invoke-DemoRequest -Method Get -Uri "/api/v1/outbox/status" | ConvertTo-Json -Depth 8
Write-Host "Latest ledger hash:"
Invoke-DemoRequest -Method Get -Uri "/api/v1/ledger/latest-hash" | ConvertTo-Json -Depth 8
Write-Host "Ledger events:"
Invoke-DemoRequest -Method Get -Uri "/api/v1/ledger" | ConvertTo-Json -Depth 8

Write-Host "`nDemo complete. Use the correlation IDs above with the application logs and observability stack."
