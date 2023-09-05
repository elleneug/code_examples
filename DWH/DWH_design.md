**Domain**

Sale of credit cards by the bank manager and the bank's client during meetings.

**Analysis tasks**
1.	Sales analytics
2.	Analysis of sales managers' performance
3.	Conduct A/B tests of sales mechanics:
  a.	Business rules
  b.	ML model

# <a name="_toc144736918"></a>**Methodology-based design**

1. ## <a name="_toc144736919"></a>**Dimensional Modeling**
![DWH_project-Anchor modeling](https://github.com/elleneug/code_courses/assets/56693605/05669f6a-02de-40ff-8724-332c2db318be)




**fct\_ss\_meetings** 

Table of events - managers' meetings with clients.


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**meeting\_id**|STRING|Meeting ID|
|client\_id|STRING|Client ID|
|manager\_id|STRING|Manager ID|
|meeting\_dt|DATETIME|Meeting date|
|action\_id|STRING|ID of actions|
|action\_start|DATETIME|Start of action|
|action\_end|DATETIME|Expiration|
|meeting\_type|INTEGER|1 - face-to-face meeting, 2 - call, 3 - online consultation|
|meeting\_result|INTEGER|1 - no answer, 2 - a repeated meeting is scheduled, 3 - a repeated call is made, 4 - a sale.|

**dim\_managers**

Showcase describing characteristics of the sales manager. A sales manager can change roles in the bank, so two identifiers are used: manager\_id, employee\_id.


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**manager\_id**|STRING|Manager ID|
|manager\_type|INTEGER|Type of manager: 1 in the bank branch, 2 in the field, 3 in the payroll|
|start\_dt|DATETIME|Job start date|
|employee\_id|STRING|Employee ID|
|REPORT\_DATE|DATETIME|Date of snapshota|

**dim\_employees**

A screen describing the characteristics of the bank's employees. 


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**employee\_id**|STRING|Employee ID|
|name|STRING|Employee's full name|
|work\_start\_dt|DATETIME|Start date|
|grade|INTEGER|Grade|
|salary|FLOAT|Salary|
|department\_head\_flg|BOOLEAN|Flag of department head|
|tribe\_head\_flg|BOOLEAN|Function Block manager flag|
|department\_id|STRING|Department ID|
|region\_id|STRING|Region ID|
|REPORT\_DATE|DATETIME|Date of snapshota|

**dim\_departments**

Screen of Level 2 organization. 


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**department\_id**|STRING|Department ID|
|description|STRING|Department name|
|tribe\_id|STRING|Function ID|

**dim\_tribes**

Screen of Level 1 organizational structure. 


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**tribe\_id**|STRING|Function ID|
|description|STRING|Function Block name|


**dim\_actions**

Showcase describing many sales actions.


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**action\_id**|STRING|ID of actions|
|action\_desc|STRING|Action description, defined by business process map|
|action\_type|STRING|Action type (1 - initial communication, 2 - consultation, 3 - sale)|
|time\_needed|FLOAT|Assessment of standard time required for sales action (to be determined by business)|

**dim\_customers**

Customer information mart.


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**client\_id**|STRING|Client ID|
|name|STRING|Client’s full name|
|passport\_no|BIGINT|Identification document number |
|doc\_valid|FLOAT|Assessment of standard time required for sales action (to be determined by business)|
|gender|STRING|F - woman, M - man, O - other/no data|
|segment|STRING|MASS - mass, VIP - premium, ULTIMA - ultimatum segment|
|date\_birth|DATETIME|Date of birth|
|region\_id|STRING|Region ID|
|resident\_flg|BOOLEAN|Resident flag (yes/no)|
|report\_date|DATETIME|Date of snapshota|

**dim\_region**

Region description mart.


|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|**region\_id**|STRING|Region ID|
|name|STRING|Region name, short|
|description|STRING|Full region name|
|population|INTEGER|Population estimation|


1. ## <a name="_toc144736920"></a>**Data Vault**

![DWH_project-Data Vault](https://github.com/elleneug/code_courses/assets/56693605/d98adca7-dfc1-4ccb-99db-a66a0ca26968)



Description of key mart attributes is similar to part I.Dimensional Modeling.

But the structure is consistent with Data\_Vault:

- Prefix s\_ - statelites;
- Prefix h\_ - hubs;
- Prefix l\_ - lines;
- Prefix b\_ - bridge.

Also, the following attributes have been added in many marts:

|**COLUMN**|**TYPE**|**DESCRIPTION**|
| - | - | - |
|load\_dttm|DATETIME|Date of information upload to source|
|source\_id|STRING|Information source ID|
|valid\_from\_dtm|DATETIME|Start of information update period|
|valid\_to\_dtm|DATETIME|Completion of information update period|

1. ## <a name="_toc144736921"></a>**Anchor Modeling**

![DWH_project-Anchor modeling](https://github.com/elleneug/code_courses/assets/56693605/9bc4494d-29c3-41b6-890a-458917aada24)



Design DWH according to Anchor Modeling:

Forecast k\_ - knots;

Prefix l\_ - links;

Prefix a\_ - anchors;
