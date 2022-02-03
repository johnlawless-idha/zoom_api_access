# Data Dictionary - Zoom API data

- there are several kinds of columns in this data, and they are numerous. The precise number is flexible, and depends on the number of participants in a given virtual visit. There are two kinds of columns, static (the same for all entries), and numbered (repeated for each participant). For numbered columns, the suffix "_n" designates the number of the user, where user 0 is the host, and user 1 is generally the patient. However, if a user signs in or out, gets disconnected, changes devices, etc, they are designated as a completely different number. In addition, interpreters, advocates, etc, may also be present. Please be sure to analyze participant data in these cases!

## Static columns

| Column Name        | Dtype    | Description                                                                                                                               |
|--------------------|----------|-------------------------------------------------------------------------------------------------------------------------------------------|
| meeting_id         | string   | Unique identifier by zoom api for a given zoom session                                                                                    |
| host_id            | string   | Unique identifier by Zoom API for the user who hosted the zoom session                                                                    |
| user_name          | string   | Host username                                                                                                                             |
| user_email         | string   | Host Zoom account email (note: BCH id is often couched in this email!)                                                                    |
| start_time         | datetime | Timestamp of meeting start time                                                                                                           |
| end_time           | datetime | Timestamp of meeting end time                                                                                                             |
| duration           | int      | Number of minutes of zoom meeting (unclear how this differs from total minutes)                                                           |
| total_minutes      | int      | Number of minutes of zoom meeting (unclear how this differs from duration)                                                                |
| participants_count | int      | Number of users who joined the zoom session (note: users who sign out and back in, switch devices, etc, are counted as a new participant) |
| epic_csn           | string   | Unique Identifier: contact serial number for the virtual visit in EPIC                                                                    |

## Numbered columns - participants 
- for each user in a zoom meeting, the following participant data is recorded, ending with _n to designate that it will be a numbered entry
| Column Name             | Dtype    | Description                                                                                                                                         |
|-------------------------|----------|-----------------------------------------------------------------------------------------------------------------------------------------------------|
| id_n                    | string   | Unique Identifier for a zoom user account (host's id should match host_id, and the same account will  have the same id, even if given a new number) |
| user_id_n               | int      | Unique identifier for a given sign-in (not useful for tracking users, this can be dropped)                                                          |
| user_name_n             | string   | Zoom account username (if given)                                                                                                                    |
| device_n                | string   | Method by which a user connects to zoom. Values include "Mac", "Windows", "Mobile", "iPad", and some web browser options                            |
| ip_address_n            | float    | IP address of user connecting                                                                                                                       |
| internal_ip_addresses_n | list     | collection of internal ip addresses of the user connecting, if applicable                                                                           |
| location_n              | string   | City/county/location name where user connected from                                                                                                 |
| network_type_n          | string   | Connection method of user (Wifi, wired, cellular, PPP, "others")                                                                                    |
| microphone_n            | string   | Audio input device of user                                                                                                                          |
| speaker_n               | string   | Audio output device of user                                                                                                                         |
| camera_n                | string   | Video input device of user                                                                                                                          |
| data_center_n           | string   | Uncertain: possibly zoom server that hosts video session                                                                                            |
| full_data_center_n      | string   | Uncertain: appears to duplicate previous column (can likely be discarded)                                                                           |
| connection_type_n       | string   | Method of connection - UDP, P2P, SSL + Proxy, or SSL                                                                                                |
| join_time_n             | datetime | Timestamp that user signed in to session                                                                                                            |
| leave_time_n            | datetime | Timestamp that user disconnected from session                                                                                                       |
| share_application_n     | Boolean  | True if user shared an application during session                                                                                                   |
| share_desktop_n         | Boolean  | True if user shared their screen during session                                                                                                     |
| share_whiteboard_n      | Boolean  | True if user shared a whiteboard during session                                                                                                     |
| recording_n             | Boolean  | True if user recorded all or part of the sesion                                                                                                     |
| pc_name_n               | string   | Device name (if given)                                                                                                                              |
| domain_n                | string   | Domain of zoom session (CHBOSTON.ORG if through internal domain, else user's domain)                                                                |
| mac_addr_n              | string   | Uncertain: may be related to IP address                                                                                                             |
| harddisk_id_n           | string   | Uncertain: likely able to be dropped                                                                                                                |
| version_n               | float    | Zoom version in use by user                                                                                                                         |
| status_n                | string   | Status at connection - "in meeting" or "in waiting room" - likely able to be dropped as well                                                        |
| role_n                  | string   | "Host" or "attendee" warning: not as accurate as using id. This should be dropped                                                                   |

## Numbered columns - Quality metrics