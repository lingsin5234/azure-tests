from menu import Menu, MenuItem
from django.urls import reverse
from . import views as bc_vw

# add items to the menu
Menu.add_item("azure", MenuItem("My Portfolio", url="/", weight=10))
Menu.add_item("azure", MenuItem("Azure Compute Test", reverse(bc_vw.azure_compute), weight=10))
