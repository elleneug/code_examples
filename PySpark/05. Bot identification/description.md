# Bot Identification Among User Sessions

You work at a product company in the retail sector. According to analysts' research, your sales volume has decreased due to competitors who undercut prices by offering lower ones than on your platform. The data analysts have a confirmed hypothesis of an unusual surge in user behavior outliers, which are most likely crawlers or bots. It's possible that these entities are collecting information about the prices of goods you offer compared to competitors.

You are tasked with implementing a classifier for such sessions to filter them out by showing them a CAPTCHA. However, for this, it's essential to know the suspicious session's identifier (`session_id`).

Begin by studying the structure of the data provided by the engineers, which were collected over a relatively short period and describe the behavior of your website's users.


## Data Structure

| Field             | Description                                                         |
|-------------------|---------------------------------------------------------------------|
| session_id        | Unique user session identifier                                      |
| user_type         | User type [authorized/guest]                                        |
| duration          | Session duration time (in seconds)                                  |
| platform          | User's platform [web/ios/android]                                   |
| item_info_events  | Number of item information view events per session                  |
| select_item_events| Number of item selection events per session                         |
| make_order_events | Number of order placement events for an item per session            |
| events_per_min    | Average number of events per minute during the session              |
| is_bot            | Bot indicator [0 - user, 1 - bot]                                   |

## Task

Let's implement two tasks for training the best data model and for its application.

