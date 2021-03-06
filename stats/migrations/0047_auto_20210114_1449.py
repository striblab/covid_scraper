# Generated by Django 3.1.3 on 2021-01-14 20:49

from django.db import migrations, models


class Migration(migrations.Migration):

    dependencies = [
        ('stats', '0046_vaccountydate'),
    ]

    operations = [
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_moderna_cdc_ltc',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_moderna_mn_providers',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_moderna_total',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_pfizer_cdc_ltc',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_pfizer_mn_providers',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='distributed_pfizer_total',
            field=models.IntegerField(null=True),
        ),
        migrations.AddField(
            model_name='vacadmintotaldate',
            name='providers',
            field=models.IntegerField(null=True),
        ),
    ]
