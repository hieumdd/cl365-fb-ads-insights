from models.AdsInsights.base import ads_insights_pipeline

AdsInsights = ads_insights_pipeline(
    request_options={
        "level": "ad",
        "fields": [
            "date_start",
            "date_stop",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
            "campaign_name",
            "adset_name",
            "ad_name",
            "spend",
        ],
    },
    transform=lambda rows: [
        {
            "account_id": row["account_id"],
            "date_start": row["date_start"],
            "date_stop": row["date_stop"],
            "campaign_id": row["campaign_id"],
            "adset_id": row["adset_id"],
            "ad_id": row["ad_id"],
            "campaign_name": row["campaign_name"],
            "adset_name": row["adset_name"],
            "ad_name": row["ad_name"],
            "spend": row.get("spend"),
        }
        for row in rows
    ],
    load_options={
        "name": "AdsInsights",
        "schema": [
            {"name": "account_id", "type": "NUMERIC"},
            {"name": "date_start", "type": "DATE"},
            {"name": "date_stop", "type": "DATE"},
            {"name": "campaign_id", "type": "NUMERIC"},
            {"name": "adset_id", "type": "NUMERIC"},
            {"name": "ad_id", "type": "NUMERIC"},
            {"name": "campaign_name", "type": "STRING"},
            {"name": "adset_name", "type": "STRING"},
            {"name": "ad_name", "type": "STRING"},
            {"name": "spend", "type": "NUMERIC"},
            {"name": "_batched_at", "type": "TIMESTAMP"},
        ],
        "p_key": [
            "date_start",
            "date_stop",
            "account_id",
            "campaign_id",
            "adset_id",
            "ad_id",
        ],
    },
)
