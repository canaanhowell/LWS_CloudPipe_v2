#!/usr/bin/env python3
"""
Azure Resource Summary Script for LWS CloudPipe v2
Generates a comprehensive summary of all Azure resources
"""

import subprocess
import json
import os
from datetime import datetime
from pathlib import Path

def run_az_command(command):
    """Run Azure CLI command and return JSON result"""
    try:
        result = subprocess.run(
            command,
            capture_output=True,
            text=True,
            shell=True
        )
        if result.returncode == 0:
            return json.loads(result.stdout)
        else:
            return {"error": result.stderr}
    except Exception as e:
        return {"error": str(e)}

def get_resource_summary():
    """Get comprehensive Azure resource summary"""
    summary = {
        "timestamp": datetime.utcnow().isoformat(),
        "subscription": {},
        "resource_groups": [],
        "storage_accounts": [],
        "container_apps": [],
        "container_registries": [],
        "logic_apps": [],
        "app_service_plans": [],
        "web_apps": [],
        "monitoring": [],
        "managed_identities": []
    }
    
    # Get subscription info
    print("🔍 Getting subscription information...")
    subscription = run_az_command("az account show")
    if "error" not in subscription:
        summary["subscription"] = {
            "name": subscription.get("name"),
            "id": subscription.get("id"),
            "tenant_id": subscription.get("tenantId"),
            "state": subscription.get("state")
        }
    
    # Get resource groups
    print("📁 Getting resource groups...")
    resource_groups = run_az_command("az group list --output json")
    if isinstance(resource_groups, list):
        summary["resource_groups"] = [
            {
                "name": rg.get("name"),
                "location": rg.get("location"),
                "tags": rg.get("tags", {})
            }
            for rg in resource_groups
        ]
    
    # Get storage accounts
    print("💾 Getting storage accounts...")
    storage_accounts = run_az_command("az storage account list --output json")
    if isinstance(storage_accounts, list):
        summary["storage_accounts"] = [
            {
                "name": sa.get("name"),
                "resource_group": sa.get("resourceGroup"),
                "location": sa.get("location"),
                "kind": sa.get("kind"),
                "sku": sa.get("sku", {}).get("name"),
                "status": sa.get("statusOfPrimary")
            }
            for sa in storage_accounts
        ]
    
    # Get container apps
    print("🐳 Getting container apps...")
    container_apps = run_az_command("az containerapp list --output json")
    if isinstance(container_apps, list):
        summary["container_apps"] = [
            {
                "name": ca.get("name"),
                "resource_group": ca.get("resourceGroup"),
                "location": ca.get("location"),
                "fqdn": ca.get("properties", {}).get("configuration", {}).get("ingress", {}).get("fqdn"),
                "revision": ca.get("properties", {}).get("latestRevisionName"),
                "status": ca.get("properties", {}).get("runningStatus")
            }
            for ca in container_apps
        ]
    
    # Get container registries
    print("📦 Getting container registries...")
    registries = run_az_command("az acr list --output json")
    if isinstance(registries, list):
        summary["container_registries"] = [
            {
                "name": reg.get("name"),
                "resource_group": reg.get("resourceGroup"),
                "location": reg.get("location"),
                "login_server": reg.get("loginServer"),
                "sku": reg.get("sku", {}).get("name")
            }
            for reg in registries
        ]
    
    # Get logic apps
    print("⚡ Getting logic apps...")
    logic_apps = run_az_command("az logic workflow list --output json")
    if isinstance(logic_apps, list):
        summary["logic_apps"] = [
            {
                "name": la.get("name"),
                "resource_group": la.get("resourceGroup"),
                "location": la.get("location"),
                "state": la.get("state")
            }
            for la in logic_apps
        ]
    
    # Get app service plans
    print("🏗️ Getting app service plans...")
    app_plans = run_az_command("az appservice plan list --output json")
    if isinstance(app_plans, list):
        summary["app_service_plans"] = [
            {
                "name": plan.get("name"),
                "resource_group": plan.get("resourceGroup"),
                "location": plan.get("location"),
                "sku": plan.get("sku", {}).get("name"),
                "kind": plan.get("kind")
            }
            for plan in app_plans
        ]
    
    # Get web apps
    print("🌐 Getting web apps...")
    web_apps = run_az_command("az webapp list --output json")
    if isinstance(web_apps, list):
        summary["web_apps"] = [
            {
                "name": app.get("name"),
                "resource_group": app.get("resourceGroup"),
                "location": app.get("location"),
                "state": app.get("state"),
                "default_host_name": app.get("defaultHostName")
            }
            for app in web_apps
        ]
    
    # Get monitoring resources
    print("📊 Getting monitoring resources...")
    insights = run_az_command("az monitor app-insights component list --output json")
    if isinstance(insights, list):
        summary["monitoring"] = [
            {
                "name": insight.get("name"),
                "resource_group": insight.get("resourceGroup"),
                "location": insight.get("location"),
                "application_type": insight.get("kind"),
                "connection_string": insight.get("connectionString") is not None
            }
            for insight in insights
        ]
    
    # Get managed identities
    print("🔐 Getting managed identities...")
    identities = run_az_command("az identity list --output json")
    if isinstance(identities, list):
        summary["managed_identities"] = [
            {
                "name": identity.get("name"),
                "resource_group": identity.get("resourceGroup"),
                "location": identity.get("location"),
                "principal_id": identity.get("principalId"),
                "tenant_id": identity.get("tenantId")
            }
            for identity in identities
        ]
    
    return summary

def save_summary(summary, output_file="azure_resource_summary.json"):
    """Save summary to JSON file"""
    output_path = Path("logs") / output_file
    output_path.parent.mkdir(exist_ok=True)
    
    with open(output_path, 'w') as f:
        json.dump(summary, f, indent=2)
    
    return output_path

def print_summary(summary):
    """Print a formatted summary to console"""
    print("\n" + "="*80)
    print("🚀 AZURE RESOURCE SUMMARY - LWS CloudPipe v2")
    print("="*80)
    print(f"📅 Generated: {summary['timestamp']}")
    
    if summary["subscription"]:
        sub = summary["subscription"]
        print(f"\n📋 SUBSCRIPTION:")
        print(f"   Name: {sub.get('name', 'N/A')}")
        print(f"   ID: {sub.get('id', 'N/A')}")
        print(f"   State: {sub.get('state', 'N/A')}")
    
    print(f"\n📁 RESOURCE GROUPS: {len(summary['resource_groups'])}")
    for rg in summary["resource_groups"]:
        print(f"   • {rg['name']} ({rg['location']})")
    
    print(f"\n💾 STORAGE ACCOUNTS: {len(summary['storage_accounts'])}")
    for sa in summary["storage_accounts"]:
        print(f"   • {sa['name']} ({sa['location']}) - {sa['sku']}")
    
    print(f"\n🐳 CONTAINER APPS: {len(summary['container_apps'])}")
    for ca in summary["container_apps"]:
        print(f"   • {ca['name']} ({ca['location']}) - {ca['status']}")
    
    print(f"\n📦 CONTAINER REGISTRIES: {len(summary['container_registries'])}")
    for reg in summary["container_registries"]:
        print(f"   • {reg['name']} ({reg['location']}) - {reg['login_server']}")
    
    print(f"\n⚡ LOGIC APPS: {len(summary['logic_apps'])}")
    for la in summary["logic_apps"]:
        print(f"   • {la['name']} ({la['location']}) - {la['state']}")
    
    print(f"\n🌐 WEB APPS: {len(summary['web_apps'])}")
    for app in summary["web_apps"]:
        print(f"   • {app['name']} ({app['location']}) - {app['state']}")
    
    print(f"\n📊 MONITORING: {len(summary['monitoring'])}")
    for monitor in summary["monitoring"]:
        print(f"   • {monitor['name']} ({monitor['location']}) - {monitor['application_type']}")
    
    print(f"\n🔐 MANAGED IDENTITIES: {len(summary['managed_identities'])}")
    for identity in summary["managed_identities"]:
        print(f"   • {identity['name']} ({identity['location']})")
    
    print("\n" + "="*80)

def main():
    """Main function"""
    print("🚀 Starting Azure Resource Summary Generation...")
    
    # Check if Azure CLI is available
    try:
        result = subprocess.run(["az", "--version"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Azure CLI not found or not working properly")
            return
    except FileNotFoundError:
        print("❌ Azure CLI not installed")
        return
    
    # Check if logged in
    try:
        result = subprocess.run(["az", "account", "show"], capture_output=True, text=True)
        if result.returncode != 0:
            print("❌ Not logged into Azure. Please run 'az login' first")
            return
    except Exception as e:
        print(f"❌ Error checking Azure login: {e}")
        return
    
    # Generate summary
    summary = get_resource_summary()
    
    # Save to file
    output_path = save_summary(summary)
    
    # Print summary
    print_summary(summary)
    
    print(f"\n✅ Summary saved to: {output_path}")
    print("🎯 Azure Resource Summary Complete!")

if __name__ == "__main__":
    main() 