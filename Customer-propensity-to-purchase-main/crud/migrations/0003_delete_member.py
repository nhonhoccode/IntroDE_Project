# Generated by Django 5.0.6 on 2024-05-22 17:54

from django.db import migrations


class Migration(migrations.Migration):
    dependencies = [
        ("crud", "0002_alter_isselect_not_select_alter_isselect_select"),
    ]

    operations = [
        migrations.DeleteModel(
            name="Member",
        ),
    ]
