#!/usr/bin/env pwsh
<#
.SYNOPSIS
    Delegates to start-all.ps1 for centralized boot and test orchestration.
#>
$ErrorActionPreference = "Stop"
$ScriptDir = Split-Path -Parent $MyInvocation.MyCommand.Path
& "$ScriptDir/start-all.ps1" @args
Write-Host "Run the following command to watch your pods spin up:"
Write-Host "    kubectl get pods -w"
Write-Host "`nOnce the Grafana pod is ready, access the dashboard at http://localhost:3000 by running:"
Write-Host "    kubectl port-forward svc/eventdrivenmicroservices-grafana 3000:3000"

