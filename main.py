from app.config import load_config
from app.http_client import WishlistApiClient
from app.validator import WishlistValidator
import json


def main() -> None:
    config = load_config()

    client = WishlistApiClient(config)
    validator = WishlistValidator()

    
    raw_items = client.get_all_wishlists(rows=config.default_rows)

    result = validator.validate_data(raw_items)

    #print(f"Fetched: {len(raw_items)}")
    #print(f"Valid:   {len(result.valid)}")
    #print(f"Invalid: {len(result.invalid)}")

    print(json.dumps([w.model_dump() for w in result.valid], indent=2, ensure_ascii=False, default=str))

    # Show first error if any (good for debugging)
    if result.invalid:
        first = result.invalid[0]
        print("\nFirst validation failure:")
        print(f"  wishlist_id: {first.wishlist_id}")
        print(f"  error: {first.error}")


if __name__ == "__main__":
    main()