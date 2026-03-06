import json
from app.config.gift_reggie_config import load_gift_reggie_config
from app.clients.gift_reggie_http_client import WishlistApiClient
from app.services.validator import WishlistValidator

from dataclasses import asdict

from app.config.db_connection_config import load_db_config

from app.clients.db_client import PostgresClient

from app.transforms.raw_wishlist_transform import build_raw_wishlist_rows_data, utc_now
from app.repository.raw_wishlists_repository import RawWishlistsRepository


def main() -> None:

    # -----------------------------
    # Load configs
    # -----------------------------
    gift_reggie_config = load_gift_reggie_config()
    #db_config = load_db_config()

    # -----------------------------
    # Initialize clients
    # -----------------------------

    gift_reggie_client = WishlistApiClient(gift_reggie_config)
    #db_client = PostgresClient(db_config)



    # -----------------------------
    # Fetch API data from Gift Reggie
    # -----------------------------
    
    incoming_raw_items = gift_reggie_client.get_all_wishlists(rows=gift_reggie_config.default_rows)

    # -----------------------------
    # Validate Incoming Data
    # -----------------------------
    gift_reggie_data_validator = WishlistValidator()
    GR_validation_result = gift_reggie_data_validator.validate_data(incoming_raw_items)

    # -----------------------------
    # For Testing Purpose
    # -----------------------------

    #print(f"Fetched: {len(incoming_raw_items)}")
    #print(f"Valid:   {len(GR_validation_result.valid)}")
    #print(f"Invalid: {len(GR_validation_result.invalid)}")

    #print(json.dumps([w.model_dump() for w in GR_validation_result.valid], indent=2, ensure_ascii=False, default=str))

    # Show first error if any (good for debugging)
    # if GR_validation_result.invalid:
    #     first = GR_validation_result.invalid[0]
    #     print("\nFirst validation failure:")
    #     print(f"  wishlist_id: {first.wishlist_id}")
    #     print(f"  error: {first.error}")

    print("\nValidation Completed\n")

    # -----------------------------
    # Transform into DB rows
    # -----------------------------
    

    run_id = 1 #
    sync_time = utc_now()

    raw_wishlist_table_rows = build_raw_wishlist_rows_data(
        wishlists=GR_validation_result.valid,
        run_id=run_id,
        synced_at=sync_time,
    )

    # -----------------------------
    # For Db Testing Purpose
    # -----------------------------
    
    print(json.dumps([asdict(row) for row in raw_wishlist_table_rows], indent=2, default=str))
    #print(json.dumps([asdict(row) for row in raw_wishlist_table_rows[:3]], indent=2, default=str))

    # -----------------------------
    # Connect to DB
    # -----------------------------

    #db_connection = db_client.create_connection()

    # try:
    #     raw_wishlist_table_repo = RawWishlistsRepository(db_connection)

    #     #print("Writing rows to database...")
    #     raw_wishlist_table_repo.upsert_rows(raw_wishlist_table_rows)

    #     db_connection.commit()
    #     print(f"Successfully upserted {len(raw_wishlist_table_rows)} rows into raw_gr_wishlists")

    # except Exception as e:
    #     print("Error occurred during DB write. Rolling back transaction.")
    #     db_connection.rollback()
    #     raise e

    # finally:
    #     print("Closing database connection")
    #     db_connection.close()

    #print("Pipeline completed successfully")




if __name__ == "__main__":
    main()