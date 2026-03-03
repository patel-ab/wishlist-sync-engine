from datetime import datetime
from typing import List, Optional

from pydantic import BaseModel, Field, ConfigDict



# Owner Object
class OwnerDTO(BaseModel):

    email: Optional[str] = None
    id: Optional[int] = None  # Gift Reggie internal user ID
    first_name: Optional[str] = None
    last_name: Optional[str] = None

    # Need to discuss with team
    model_config = ConfigDict(extra="allow") 
    # extra="forbid" means:
    # If API sends unexpected fields, validation fails.
    # This protects against silent contract changes.


# Product Objects
class WishlistProductDTO(BaseModel):
    """
    Represents each product in the 'products' array.
    """

    id: int  # Wishlist product ID (required)

    product_id: Optional[int] = None
    variant_id: Optional[int] = None
    sku: Optional[str] = None
    image: Optional[str] = None
    inventory_quantity: Optional[int] = None
    tags: Optional[str] = None

     # Need to discuss with team
    model_config = ConfigDict(extra="allow")


# Main response handling model
class WishlistDTO(BaseModel):
    """
    Represents a single wishlist returned by GET /wishlists.
    """

    id: int  # Wishlist ID (required)

    created: datetime

    # Gift Reggie seems to send "updated" too, so we model it
    updated: Optional[datetime] = None

    extra: Optional[str] = None

    owner: Optional[OwnerDTO] = None

    products: List[WishlistProductDTO] = Field(default_factory=list)

    model_config = ConfigDict(extra="forbid")