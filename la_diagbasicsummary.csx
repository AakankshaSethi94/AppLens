using System.Text;

private static string clusterName = "processus";
private static string startTime = "";
private static string endTime = "";
private static string subscriptionId = "";
private static string resourceGroup = "";
private static string workflowName = "";

[LogicAppFilter(InternalOnly = true)]
[Definition(Id = "la_diagbasicsummary", Name = "Basic Summary", Category = "Diagnostics", Author = "aalevato", Description = "LogicApp Diagnostics -  General Workflow Summary")]
public async static Task<Response> Run(DataProviders dp, OperationContext<LogicApp> cxt, Response res)
{
    startTime = cxt.StartTime;
    endTime = cxt.EndTime;
    subscriptionId = cxt.Resource.SubscriptionId;
    resourceGroup = cxt.Resource.ResourceGroup;
    workflowName = cxt.Resource.Name;

    var definition = await GetLastDefinition(dp);
    var sourceNamespace = definition["SourceNamespace"].ToString();
    var scaleUnit = definition["scaleUnit"].ToString();
    var referencedApiNames = definition["referencedApiNames"].ToString();
    var referencedApiActionHosts = definition["referencedApiActionHosts"].ToString();
    var usedPrimitiveTypes = definition["usedPrimitiveTypes"].ToString();
    var changedTime = definition["changedTime"].ToString();

    var totalRuns = await GetTotalRunsCount(dp);
    var throttledRuns = await GetThrottledRunsCount(dp);

    var summaries = await GetDataSummaries(dp, totalRuns, throttledRuns);
    var list = new List<DataSummary>();

    AddAsDataSummary(summaries, list, "Total", "darkblue");
    AddAsDataSummary(summaries, list, "Started", "blue");
    AddAsDataSummary(summaries, list, "Succeeded", "darkgreen");
    AddAsDataSummary(summaries, list, "Failed", "darkred");
    AddAsDataSummary(summaries, list, "Throttled", "orange");

    res.AddDataSummary(list, "LogicApp Executions on the Period (based on Working Actions)", "These counts may include runs that started prior to the period or that didn't finish before the end of the period");

    var insights = await GetLastFailedRunsInsights(dp);

    if (Convert.ToInt32(throttledRuns) > 0)
    {
        insights.Add(new Insight(
            InsightStatus.Critical, 
            $"A total of {throttledRuns} runs had throttled actions in the period",
            await GetThrottledRunsDetails(dp)));
    }
    else 
    {
        insights.Add(new Insight(
            InsightStatus.Success, 
            "No runs had throttled actions in the period"));
    }

    if (Convert.ToDateTime(changedTime) >= (Convert.ToDateTime(startTime) - TimeSpan.FromDays(7)))
    {
        insights.Add(new Insight(
            InsightStatus.Critical, 
            "The workflow versions in use during this period were changed within the week before the start time", 
            await GetDefinitionsList(dp)));
    }
    else 
    {
        insights.Add(new Insight(
            InsightStatus.Success, 
            "All versions of this workflow that executed during this period were created over a week before the start time", 
            await GetDefinitionsList(dp)));
    }

    var jobErrorsDetails = await GetJobErrorsList(dp);
    if (jobErrorsDetails.Count > 0)
    {
        insights.Add(new Insight(
            InsightStatus.Critical, 
            "Some correlations were found between job errors and actions happened during this period", 
            jobErrorsDetails));
    }
    else 
    {
        insights.Add(new Insight(
            InsightStatus.Success, 
            "No correlation was found between job errors and actions happened during this period"));
    }

    res.AddInsights(insights);

    return res;
}

private static string GetFullTableName(string tableName)
{
    return $"cluster('{clusterName}').database('process').{tableName}";
} 

private static async Task<DataRow> GetLastDefinition(DataProviders dp)
{
    var clusters = new string[] 
    { 
        "processus",        "processeurope",    "processasia", 
        "processaustralia", "processbrazil",    "processcanada", 
        "processjapan" 
    };
    foreach (var cluster in clusters)
    {
        clusterName = cluster;
        var table = await dp.Kusto.ExecuteClusterQuery(
            $@" {GetFullTableName("WorkflowDefinitions")}
            | where todatetime(changedTime) <= datetime({endTime})
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where workflowName =~ '{workflowName}'
            | project SourceNamespace, scaleUnit, referencedApiActionHosts, referencedApiNames, usedPrimitiveTypes, changedTime
            | top 1 by changedTime desc");
        if (table.Rows.Count > 0)
        {
            return table.Rows[0];
        }
    }
    clusterName = "processus";
    return null;
}

private static void AddAsDataSummary(Dictionary<string, string> summaries, List<DataSummary> list, string key, string color)
{
    if (summaries.ContainsKey(key))
    {
        list.Add(new DataSummary(key, summaries[key], color));
    }
    else 
    {
        list.Add(new DataSummary(key, "0", color));
    }
}

private static async Task<Dictionary<string, string>> GetDataSummaries(DataProviders dp, string totalRuns, string throttledRuns)
{
    var summaries = new Dictionary<string, string>();
    var table = await GetRunTaskStatusCounts(dp);
    foreach (DataRow row in table.Rows)
    {
        var cellTaskName = row["TaskName"].ToString();
        var cellStatus = row["status"].ToString();
        var cellCount = row["count_"].ToString();
        if (cellTaskName == "WorkflowRunStart")
        {
            summaries.Add("Started", cellCount);
        }
        else 
        {
            summaries.Add(cellStatus, cellCount);
        }
    }
    summaries.Add("Total", totalRuns);
    summaries.Add("Throttled", throttledRuns);
    return summaries;
}

private static async Task<Dictionary<string, string>> GetDefinitionsList(DataProviders dp)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowDefinitions")}
        | where todatetime(changedTime) <= datetime(2018-07-17 21:45:00)
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where workflowName =~ '{workflowName}'
        | distinct changedTime, referencedApiNames, referencedApiActionHosts, usedPrimitiveTypes
        | top 10 by changedTime desc");
    var list = new Dictionary<string, string>();
    foreach (DataRow row in table.Rows)
    {
        var changedTime = row["changedTime"].ToString();
        var referencedApiNames = row["referencedApiNames"].ToString();
        var referencedApiActionHosts = row["referencedApiActionHosts"].ToString();
        var usedPrimitiveTypes = row["usedPrimitiveTypes"].ToString();
        list.Add(changedTime, $@"<markdown>
            Referenced API Names: _{(string.IsNullOrEmpty(referencedApiNames) ? "(none)" : referencedApiNames)}_

            Referenced API Hosts: _{(string.IsNullOrEmpty(referencedApiActionHosts) ? "(none)" : referencedApiActionHosts)}_
            
            Used Primitive Types: _{(string.IsNullOrEmpty(usedPrimitiveTypes) ? "(none)" : usedPrimitiveTypes)}_
            </markdown>");
    }
    return list;
}

private static async Task<Dictionary<string, string>> GetJobErrorsList(DataProviders dp)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("JobErrors")}
            | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime}) 
            | where subscriptionId =~ '{subscriptionId}' 
            | summarize message = max(message) by correlationId, exception 
            | sort by correlationId, exception, message 
            | join ( {GetFullTableName("WorkflowActions")} 
                | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime}) 
                | where subscriptionId =~ '{subscriptionId}' 
                | where resourceGroup =~ '{resourceGroup}' 
                | where flowName =~ '{workflowName}' 
                | distinct correlationId, TIMESTAMP, actionName, status, statusCode, error 
                | join ( {GetFullTableName("WorkflowActions")} 
                    | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime}) 
                    | where subscriptionId =~ '{subscriptionId}' 
                    | where resourceGroup =~ '{resourceGroup}' 
                    | where flowName =~ '{workflowName}' 
                    | summarize TIMESTAMP = min(TIMESTAMP) by correlationId ) on correlationId and TIMESTAMP 
                | project correlationId, TIMESTAMP, actionName, status, statusCode, error ) on correlationId 
            | summarize count(correlationId) by exception, actionName
            | top 10 by count_correlationId desc");
    var list = new Dictionary<string, string>();
    var index = 0;
    foreach (DataRow row in table.Rows)
    {
        index++;
        var countCorrelationId = row["count_correlationId"].ToString();
        var exceptionText = row["exception"].ToString();
        var actionName = row["actionName"].ToString();
        list.Add(
            $"Exception {index}", 
            $@"<markdown>
            For this exception, there are {countCorrelationId} instances of correlation Ids in the Job Errors table related to actions of this workflow. The correlation usually starts with the action named '{actionName}'.

            That's the exception text:

            ```
            {exceptionText}
            ```
            </markdown>");
    }
    return list;
}

private static async Task<DataTable> GetRunTaskStatusCounts(DataProviders dp)
{
    return await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
            | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | distinct flowRunSequenceId
            | join (
                {GetFullTableName("WorkflowRuns")}
                | where subscriptionId =~ '{subscriptionId}'
                | where resourceGroup =~ '{resourceGroup}'
                | where flowName =~ '{workflowName}'
                ) on flowRunSequenceId 
            | summarize count() by TaskName, status");
}

private static async Task<string> GetTotalRunsCount(DataProviders dp)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | distinct flowRunSequenceId
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | distinct flowRunSequenceId
            ) on flowRunSequenceId
        | count");
    if (table.Rows.Count == 0)
    {
        return "0";
    }
    return table.Rows[0][0].ToString();
}

private static async Task<string> GetThrottledRunsCount(DataProviders dp)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where TaskName =~ 'WorkflowActionExecutionThrottled'
        | distinct flowRunSequenceId
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | distinct flowRunSequenceId
            ) on flowRunSequenceId
        | count");
    if (table.Rows.Count == 0)
    {
        return "0";
    }
    return table.Rows[0][0].ToString();
}

private static async Task<Dictionary<string, string>> GetThrottledRunsDetails(DataProviders dp)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where TaskName =~ 'WorkflowActionExecutionThrottled'
        | distinct flowRunSequenceId, actionName, actionType
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | distinct flowRunSequenceId
            ) on flowRunSequenceId
        | summarize count(flowRunSequenceId) by actionName, actionType
        | top 10 by count_flowRunSequenceId desc
        | project strcat(actionName, "" ("", actionType, "", "", count_flowRunSequenceId, "" runs)"")");
    var details = new Dictionary<string, string>();
    details.Add("Most Commonly Throttled Actions", GetCommaListFromTable(table, "Column1"));
    return details;
}

private static async Task<List<Insight>> GetLastFailedRunsInsights(DataProviders dp)
{
    var failedRunSummaries = await GetFailedRunSummaries(dp);
    var insights = new List<Insight>();
    if (failedRunSummaries.Rows.Count > 0)
    {
        foreach (DataRow failedRunSummary in failedRunSummaries.Rows)
        {
            var runStatusCode = failedRunSummary["statusCode"].ToString();
            var runCount = failedRunSummary["count_"].ToString();
            var message = string.Empty;
            var insightDetails = new Dictionary<string, string>();       
            if (runStatusCode != "ActionFailed")
            {
                insightDetails.Add(
                    "Last Failed Runs", 
                    await GetLastFailedRunsFromRuns(dp, runStatusCode));
            }
            switch (runStatusCode)
            {
                case "ActionFailed":
                    await AddFailedActionSummaries(dp, insights);
                    break;
                case "WorkflowRunTimedOut":
                    message = $"A total of {runCount} runs timed out";
                    break;
                case "Terminated":
                    message = $"A total of {runCount} runs were terminated";
                    break;
                case "OutputParameterEvaluationFailed":
                    message = $"A total of {runCount} runs failed to evaluate an output parameter";
                    break;
                default:
                    message = $"A total of {runCount} runs had unknown issues";
                    break;
            }
            if (!string.IsNullOrEmpty(message))
            {
                insights.Add(new Insight(InsightStatus.Critical, message, insightDetails));
            }
        }
    }
    else 
    {
        insights.Add(new Insight(InsightStatus.Success, "No failed runs on the period"));
    }
    return insights;
}

private static async Task AddFailedActionSummaries(DataProviders dp, List<Insight> insights)
{
    var failedActionSummaries = await GetFailedActionSummaries(dp);
    foreach (DataRow failedRunSummary in failedActionSummaries.Rows)
    {
        var actionStatusCode = failedRunSummary["statusCode"].ToString();
        var actionCount = failedRunSummary["count_"].ToString();
        var message = string.Empty;
        var insightDetails = new Dictionary<string, string>();
        insightDetails.Add(
            "Last Failed Runs", 
            await GetLastFailedRunsFromActions(dp, actionStatusCode));
        insightDetails.Add(
            "Most Frequently Failed Actions",
            await GetFailedActionNamesByStatusCode(dp, actionStatusCode));
        switch (actionStatusCode)
        {
            case "429":
                message = $"A total of {actionCount} runs had actions failed with 'Too Many Requests' (HTTP error 429)";
                insightDetails.Add(
                    "Common Causes",
                    "The action exceeded the limit of calls per connection allowed by the connector and is being throttled");
                insightDetails.Add(
                    "Possible Resolutions",
                    @"<markdown>
                    Consider increasing your connections, or keep the quantity of calls under the connector limits.
                    For more information, please <a href='https://docs.microsoft.com/en-us/connectors' target='_blank'>click here</a> and check the `API calls per connection` for the connector used in the failing action.
                    </markdown>");
                break;
            default:
                message = $"A total of {actionCount} runs had actions failed with '{actionStatusCode}'";
                break;
        }
        insights.Add(new Insight(InsightStatus.Critical, message, insightDetails));
    }
    await AddFailedActionRunsWithNoFailedActions(dp, insights);
}

private static async Task<DataTable> GetFailedRunSummaries(DataProviders dp)
{
    return await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowRuns")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where status =~ 'Failed'
        | summarize count() by statusCode
        | sort by count_ desc");
}

private static async Task<string> GetLastFailedRunsFromRuns(DataProviders dp, string runStatusCode)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowRuns")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where status =~ 'Failed'
        | where statusCode == '{runStatusCode}' 
        | distinct flowRunSequenceId, TIMESTAMP
        | top 3 by TIMESTAMP desc");
    return GetCommaListFromTable(table, "flowRunSequenceId");
}

private static async Task<string> GetLastFailedRunsFromActions(DataProviders dp, string actionStatusCode)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where status in ('Failed', 'TimedOut')
        | where statusCode == '{actionStatusCode}' 
        | distinct flowRunSequenceId, TIMESTAMP
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | where status =~ 'Failed'
            ) on flowRunSequenceId 
        | top 3 by TIMESTAMP desc");
    return GetCommaListFromTable(table, "flowRunSequenceId");
}

private static async Task<DataTable> GetFailedActionSummaries(DataProviders dp)
{
    return await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where status in ('Failed', 'TimedOut')
        | project flowRunSequenceId, statusCode
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | where status =~ 'Failed'
            ) on flowRunSequenceId 
        | summarize count() by statusCode
        | sort by count_ desc");
}

private static async Task AddFailedActionRunsWithNoFailedActions(DataProviders dp, List<Insight> insights)
{ 
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")} 
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}' 
        | summarize min(status) by flowRunSequenceId
        | where min_status in ('', 'Succeeded')
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}' 
            | where status =~ 'Failed'
            | distinct flowRunSequenceId
            ) on flowRunSequenceId 
        | project flowRunSequenceId
        | limit 101");
    if (table.Rows.Count > 0)
    {
        var details = new Dictionary<string, string>();
        details.Add("Last Failed Runs", GetCommaListFromTable(table, "flowRunSequenceId", 100));
        insights.Add(new Insight(
            InsightStatus.Critical, 
            table.Rows.Count > 100 
                ? "More than 100 runs failed with a result of 'ActionFailed', however no related failed actions were found" 
                : $"A total of {table.Rows.Count} runs failed with a result of 'ActionFailed', however no related failed actions were found", 
            details));
    }
}

private static async Task<string> GetFailedActionNamesByStatusCode(DataProviders dp, string actionStatusCode)
{
    var table = await dp.Kusto.ExecuteClusterQuery(
        $@" {GetFullTableName("WorkflowActions")}
        | where TIMESTAMP >= datetime({startTime}) and TIMESTAMP <= datetime({endTime})
        | where subscriptionId =~ '{subscriptionId}'
        | where resourceGroup =~ '{resourceGroup}'
        | where flowName =~ '{workflowName}'
        | where status in ('Failed', 'TimedOut')
        | where statusCode =~ '{actionStatusCode}'
        | project flowRunSequenceId, actionName
        | join (
            {GetFullTableName("WorkflowRuns")}
            | where subscriptionId =~ '{subscriptionId}'
            | where resourceGroup =~ '{resourceGroup}'
            | where flowName =~ '{workflowName}'
            | where status =~ 'Failed'
            ) on flowRunSequenceId 
        | summarize count() by actionName
        | top 3 by count_ desc");
    return GetCommaListFromTable(table, "actionName");
}

private static string GetCommaListFromTable(DataTable table, string fieldName, int limit = 1000)
{
    if (table.Rows.Count.Equals(0))
    {
        return "(none found)";
    }
    else
    {
        var list = new List<string>();
        foreach (DataRow row in table.Rows)
        {
            list.Add(row[fieldName].ToString());
            if (list.Count >= limit)
            {
                return string.Join(", ", list.ToArray());
            }
        }
        return string.Join(", ", list.ToArray());
    }
}