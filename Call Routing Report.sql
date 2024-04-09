select  c.*,
        CONVERT(VARCHAR(5), InvocaCallDateTime, 108) as InvocaCallHourMinute,
		coalesce(c2.StateIndex, 0) as StateIndex,
		coalesce(c2.StateName, 'Missing') as StateName,
		coalesce(c2.StateDurationSeconds, 0) as StateDurationSeconds,
                coalesce(endreason, 'Missing') as EndReason,
		coalesce(c2.SkillName, 'Missing') as StateSkillName,
		t.HourFromTo12 as Interval

		into #table
  
from analytic.calls c
left join Integration.CXoneContactsStateHistory c2 on c2.ContactID = c.agentsystemcallid
left join cxone.Contacts c3 on c3.ContactID = c.AgentSystemCallID
left join edw.DimTime t on t.Notation24 = CONVERT(VARCHAR(5), InvocaCallDateTime, 108)
where datemodel >= '2023-09-01'
and c.invocacallid!= 'Missing'
and channel in ('TV', 'Direct Mail', 'Digital', 'Third Party')
and (c.AgentSystemCallID = c3.ContactID or c.AgentSystemCallID = 'Missing')


select   datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# Gross Calls' as MeasureName,
		 1 as orderby,
		 count(distinct InvocaCallID) as MeasureAmount


from #table
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# Left Invoca Calls' as MeasureName,
		 2 as orderby,
		 count(distinct InvocaCallID) as LeftInvocaCalls

from #table
where AgentSystemCallID != 'Missing'
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# CXone IVR Calls' as MeasureName,
		 3 as orderby,
		 count(distinct InvocaCallID) as IVRCalls

from #table
where StateIndex = 1 and StateName = 'PreQueue'
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# InQueue Calls' as MeasureName,
		 4 as orderby,
		 count(distinct InvocaCallID) as IVRCalls

from #table
where StateIndex = 2 and StateName = 'InQueue'
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# Routed to Agent Calls' as MeasureName,
		 5 as orderby,
		 count(distinct InvocaCallID) as RoutedCalls

from #table
where StateIndex = 3 and StateName = 'Routing'
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# Handled Calls' as MeasureName,
		 6 as orderby,
		 count(distinct InvocaCallID) as ActiveCalls

from #table
where StateIndex = 4 and StateName = 'Active'
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

union all

select datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign,
		 '# Paid & Handled Calls' as MeasureName,
		 7 as orderby,
		 count(distinct InvocaCallID) as PaidHandledCalls

from #table
where StateIndex = 4 and StateName = 'Active' and PaidHandledIndicator = 1
group by datemodel,
         Channel,
		 Interval,
		 InvocaAdvertiserCampaign

drop table #table