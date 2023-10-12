import os
import datetime
import numpy as np
from flipside import Flipside
import pandas as pd
import plotly.express as px
import streamlit as st


def auto_paginate_result(query_result_set, page_size=10000):
    """
    This function auto-paginates a query result to get all the data. It assumes 10,000 rows per page.
    In case of an error, reduce the page size. Uses numpy.
    """
    num_rows = query_result_set.page.totalRows
    page_count = np.ceil(num_rows / page_size).astype(int)
    all_rows = []
    current_page = 1
    while current_page <= page_count:
        results = flipside.get_query_results(
            query_result_set.query_id,
            page_number=current_page,
            page_size=page_size
        )

        if results.records:
            all_rows.extend(results.records)  # Use extend() to add list elements

        current_page += 1  # Increment the current page number

    return all_rows  # Return all_rows in JSON format


def metawin_filter_df(df, time_period):
    if time_period == 'Last 7 days':
        df_filtered = df[df['tx_dt'] > today - datetime.timedelta(days=7)]
    elif time_period == 'Last month':
        df_filtered = df[df['tx_dt'] > today - datetime.timedelta(days=30)]
    elif time_period == 'Last 3 months':
        df_filtered = df[df['tx_dt'] > today - datetime.timedelta(days=90)]
    elif time_period == 'Last year':
        df_filtered = df[df['tx_dt'] > today - datetime.timedelta(days=365)]
    elif time_period == 'This year':
        df_filtered = df[df['tx_dt'] > today.replace(month=1, day=1)]
    else:
        df_filtered = df

    return df_filtered

# Get the current date
today = pd.Timestamp('today').date()


# Initialize `Flipside` with your API key and API URL
with open('api_key.txt', 'r') as file:
    # Read the first line
    api_key = file.readline().strip()  # .strip() removes newline characters

flipside = Flipside(api_key, "https://api-v2.flipsidecrypto.xyz")

# Files path
file_path = f"{os.getcwd()}\\data\\metawin_{today.year}{today.month}{today.day}"

# Banner image URL (replace with your image URL or local path)
banner_image_url = "https://streamlit-dashboards-frontends.s3.us-east-2.amazonaws.com/metawin-og2+(2).png"

# Link URL you want to associate with the banner
referral_link_url = "https://metawin.com/t/ffc8fa00"

# Set the page configuration to wide
st.set_page_config(layout="wide")

# HTML and CSS to set the linked banner
banner_style = f"""
  <style>
     .banner {{
        background-image: url("{banner_image_url}");
        background-size: cover;
        height: 255px;  # Adjust the height as needed
     }}
     .banner-link {{
        display: block;
        width: 100%;
        height: 10%;
        position: absolute;
        top: 0;
        left: 0;
     }}
  </style>
"""

# HTML code for the linked banner
linked_banner_html = f'<a href="{referral_link_url}" target="_blank" rel="noopener noreferrer" class="banner-link"></a>'

# Use the HTML/CSS to set the linked banner
st.markdown(f'<div class="banner">{linked_banner_html}</div>{banner_style}', unsafe_allow_html=True)

# Set the dash title
st.title("METAWIN Dashboard 🎰📊")

# Time period selector
time_period_options = ["Last 7 days", "Last month", "Last 3 months", "Last year", "This year", "All time"]
time_period = st.selectbox("Select time period:", time_period_options)

# Tabs
tabs = st.tabs(["Transactions 📊 & Gas Fees ⛽", "Tickets 🎫", "Users 👤"])

# Transactions and Gas Fees tab
with tabs[0]:
    # Create two columns
    col1, col2 = st.columns(2)

    # Loading Protocol Data using Flipside API (Transactions and Gas Fees)
    if os.path.exists(f"{file_path}_txs_and_gas.csv"):
        df_txs_and_gas = pd.read_csv(f"{file_path}_txs_and_gas.csv")
    else:
        STARTING_DATE = "'2022-01-01'"

        sql = f""" 
            with metawin_txs AS (
          SELECT
            *,
            concat(contract_address, '_', decoded_log:raffleId) AS raffle_id
          FROM
            ethereum.core.fact_decoded_event_logs
          WHERE
            contract_address IN (
              SELECT
                DISTINCT contract_address
              FROM
                ethereum.core.fact_decoded_event_logs
              WHERE
                decoded_log:"role" IN (
                  '0x523a704056dcd17bcf83bed8b68c59416dac1119be77755efe3bde0a64e46e0c',
                  '0xde5ee446972f4e39ab62c03aa34b2096680a875c3fdb3eb2f947cbb93341c058'
                )
                and decoded_log:"sender" = '0x3684a8007dc9df696a86b0c5c89a8032b78b5b0d'
                AND block_timestamp > {STARTING_DATE}
            )
            AND block_timestamp > {STARTING_DATE}
        )
        SELECT
          date_trunc('day', v1.block_timestamp) tx_dt,
          contract_address,
          event_name,
          count(DISTINCT v1.tx_hash) AS tot_txs_count,
          SUM(v2.tx_fee) AS tot_eth_fee,
          tot_eth_fee / tot_txs_count AS avg_gas_eth_gas_fee_paid_by_smart_contract,
          AVG(avg_gas_eth_gas_fee_paid_by_smart_contract) OVER(ORDER BY tx_dt)
        FROM
          metawin_txs v1
          JOIN ethereum.core.fact_transactions v2 ON v1.tx_hash = v2.tx_hash
        WHERE
          v2.block_timestamp > {STARTING_DATE}
        GROUP BY
          1,
          2,
          3
        """

        # Run the query against Flipside's query engine and await the results
        query_result_set = flipside.query(sql)

        df_txs_and_gas = auto_paginate_result(query_result_set)

        df_txs_and_gas = pd.DataFrame(df_txs_and_gas)

        df_txs_and_gas.to_csv(f"{file_path}_txs_and_gas.csv", ',')

    # Sorting Df values by Date in ascending order
    df_txs_and_gas = df_txs_and_gas.sort_values(by=['tx_dt'], ascending=True)

    # Convert the date column to a datetime format
    df_txs_and_gas['tx_dt'] = pd.to_datetime(df_txs_and_gas['tx_dt']).dt.date

    # Filter the data by time period
    df_txs_and_gas_filtered = metawin_filter_df(df_txs_and_gas, time_period)

    with col1:
        # Total number of transactions
        total_transaction_count = df_txs_and_gas_filtered["tot_txs_count"].sum()
        st.write("Total Number of Transactions:", total_transaction_count)

        # Plot the number of transactions per Day by event
        fig = px.bar(
            df_txs_and_gas_filtered,
            x="tx_dt",
            y="tot_txs_count",
            title="Number of Transactions by Event ({})".format(time_period),
            color="event_name",
            labels={"tx_dt": "Day", "tot_txs_count": "Number of Transactions"}
        )

        st.plotly_chart(fig)

        # Daily number of transactions by smart contract
        fig = px.bar(
            df_txs_and_gas_filtered,
            x="tx_dt",
            y="tot_txs_count",
            title="Daily Number of Transactions by Smart Contract ({})".format(time_period),
            color="contract_address",
            labels={"tx_dt": "Day", "tot_txs_count": "Number of Transactions"}
        )

        st.plotly_chart(fig)

    with col2:
        # Total volume of ETH Gas Fees
        total_eth_gas_fee = df_txs_and_gas_filtered["tot_eth_fee"].sum()
        st.write("Total ETH Gas Fees Generated:", total_eth_gas_fee)

        # Plot the volume of ETH Gas Fee per Day
        fig = px.bar(
            df_txs_and_gas_filtered,
            x="tx_dt",
            y="tot_eth_fee",
            title="Daily Volume of ETH Gas Fee ({})".format(time_period),
            color="event_name",
            labels={"tx_dt": "Day", "tot_eth_fee": "ETH"}
        )

        st.plotly_chart(fig)

        # Plot the volume of ETH Gas Fee per Day by smart contract
        fig = px.bar(
            df_txs_and_gas_filtered,
            x="tx_dt",
            y="tot_eth_fee",
            title="Daily Volume of ETH Gas Fee by Smart Contract ({})".format(time_period),
            color="contract_address",
            labels={"tx_dt": "Day", "tot_eth_fee": "ETH"}
        )

        st.plotly_chart(fig)

        # Average ETH Gas Fee paid by Event
        df_txs_and_gas_filtered["avg_eth_gas_fee_by_event"] = df_txs_and_gas_filtered["tot_eth_fee"] / \
                                                              df_txs_and_gas_filtered["tot_txs_count"]

        # Plot the Average ETH Gas Fee by Event (only EntrySold event)
        fig = px.bar(
            df_txs_and_gas_filtered,
            x="event_name",
            y="avg_eth_gas_fee_by_event",
            title="Average ETH Gas Fee by Event ({})".format(time_period),
            color="event_name",
            labels={"event_name": "Event", "avg_eth_gas_fee_by_event": "ETH"}
        )

        st.plotly_chart(fig)

        # Moving Average ETH Gas Fee by Smart Contract (only EntrySold action)

        # filtering by event_name = 'EntrySold'
        df_txs_and_gas_filtered_tickets = df_txs_and_gas_filtered[df_txs_and_gas_filtered["event_name"] == 'EntrySold']

        df_txs_and_gas_filtered_tickets["Daily_avg_eth_gas_fee_paid_by_smart_contract"] = \
            df_txs_and_gas_filtered_tickets["tot_eth_fee"] / df_txs_and_gas_filtered_tickets["tot_txs_count"]

        df_txs_and_gas_filtered_tickets["ma_eth_gas_fee"] = df_txs_and_gas_filtered_tickets.groupby('tx_dt')['Daily_avg_eth_gas_fee_paid_by_smart_contract'].transform(pd.Series.mean)

        #st.subheader("Moving Average ETH Gas Fee (only EntrySold event) ({})".format(time_period))

        # Plot the Moving Average ETH Gas Fee (only EntrySold event)
        fig = px.line(df_txs_and_gas_filtered_tickets,
                      x="tx_dt",
                      y="ma_eth_gas_fee",
                      title="Moving Average ETH Gas Fee (only EntrySold event)")

        st.plotly_chart(fig)

# Tickets tab
with tabs[1]:
    # Create two columns
    col3, col4 = st.columns(2)
    # Loading Protocol Data using Flipside API (Tickets)
    if os.path.exists(f"{file_path}_tickets.csv"):
        df_tickets = pd.read_csv(f"{file_path}_tickets.csv")
    else:
        STARTING_DATE = "'2022-01-01'"

        sql = f""" 
                    with metawin_txs AS (
          SELECT
            *
          FROM
            ethereum.core.fact_decoded_event_logs
          WHERE
            contract_address IN (
              SELECT
                DISTINCT contract_address
              FROM
                ethereum.core.fact_decoded_event_logs
              WHERE
                decoded_log:"role" = '0x523a704056dcd17bcf83bed8b68c59416dac1119be77755efe3bde0a64e46e0c'
                and decoded_log:"sender" = '0x3684a8007dc9df696a86b0c5c89a8032b78b5b0d'
                AND block_timestamp > {STARTING_DATE}
            )
            AND block_timestamp > {STARTING_DATE}
        ),
        token_price AS (
          SELECT
            CASE
              when symbol = 'WETH' THEN 'ETH'
              else symbol
            end as symbol,
            hour,
            token_address,
            decimals,
            AVG(price) AS avg_token_price_usd
          FROM
            ethereum.price.ez_hourly_token_prices
          WHERE
            hour > {STARTING_DATE}
            AND symbol IN ('WETH')  
          GROUP BY
            1,
            2,
            3,
            4
        )
        SELECT
          date_trunc('day', tx_timestamp) AS tx_dt,
          SUM(tot_token_spent) AS daily_eth_volume_tickets_sold,
          SUM(tot_usd_spent) AS daily_usd_volume_tickets_sold
        FROM
          (
            SELECT
              v1.tx_hash,
              v1.block_timestamp AS tx_timestamp,
              decoded_log:raffleId,
              decoded_log:buyer AS ticket_buyer_address,
              symbol AS payment_method_token,
              avg_token_price_usd,
              v2.amount AS tot_token_spent,
              tot_token_spent * avg_token_price_usd AS tot_usd_spent
            FROM
              metawin_txs v1
              JOIN ethereum.core.ez_eth_transfers v2 ON v1.tx_hash = v2.tx_hash
              JOIN token_price ON date_trunc('hour', v1.block_timestamp) = hour
              AND symbol = 'ETH'
            WHERE
              event_name = 'EntrySold'
          )
        GROUP BY
          1
        """

        # Run the query against Flipside's query engine and await the results
        query_result_set = flipside.query(sql)

        df_tickets = auto_paginate_result(query_result_set)

        df_tickets = pd.DataFrame(df_tickets)

        df_tickets.to_csv(f"{file_path}_tickets.csv", ',')

    # Sorting Df values by Date in ascending order
    df_tickets = df_tickets.sort_values(by=['tx_dt'], ascending=True)

    # Convert the date column to a datetime format
    df_tickets['tx_dt'] = pd.to_datetime(df_tickets['tx_dt']).dt.date

    # Filter the data by time period
    df_tickets_filtered = metawin_filter_df(df_tickets, time_period)

    with col3:

        # Total Volume ETH Tickets sold
        total_eth_tickets_sold = df_tickets_filtered["daily_eth_volume_tickets_sold"].sum()
        st.write("Total Volume ETH Tickets sold:", total_eth_tickets_sold)

        # Plot the daily volume of tickets sold
        fig = px.bar(
            df_tickets_filtered,
            x="tx_dt",
            y="daily_eth_volume_tickets_sold",
            title="Daily ETH Volume Tickets Sold ({})".format(time_period),
            labels={"tx_dt": "Day", "daily_eth_volume_tickets_sold": "ETH"}
        )

        st.plotly_chart(fig)

    with col4:

        # Total Volume USD Tickets sold
        total_usd_tickets_sold = df_tickets_filtered["daily_usd_volume_tickets_sold"].sum()
        st.write("Total Volume USD Tickets sold:", total_usd_tickets_sold)

        # Plot the daily volume of tickets sold
        fig = px.bar(
            df_tickets_filtered,
            x="tx_dt",
            y="daily_usd_volume_tickets_sold",
            title="Daily USD Volume Tickets Sold ({})".format(time_period),
            width=800,
            height=400,
            labels={"tx_dt": "Day", "daily_usd_volume_tickets_sold": "USD"}
        )

        st.plotly_chart(fig)

# Users tab
with tabs[2]:
    # Create two columns
    col1, col2 = st.columns(2)

    # Loading Protocol Data using Flipside API (Transactions and Gas Fees)
    if os.path.exists(f"{file_path}_users.csv"):
        df_users = pd.read_csv(f"{file_path}_users.csv")
    else:
        STARTING_DATE = "'2022-01-01'"

        sql = f""" 
           with metawin_txs AS (
                  SELECT
                    *,
                    concat(contract_address, '_', decoded_log:raffleId) AS raffle_id
                  FROM
                    ethereum.core.fact_decoded_event_logs
                  WHERE
                    contract_address IN (
                      SELECT
                        DISTINCT contract_address
                      FROM
                        ethereum.core.fact_decoded_event_logs
                      WHERE
                        decoded_log:"role" IN (
                          '0x523a704056dcd17bcf83bed8b68c59416dac1119be77755efe3bde0a64e46e0c',
                          '0xde5ee446972f4e39ab62c03aa34b2096680a875c3fdb3eb2f947cbb93341c058'
                        )
                        and decoded_log:"sender" = '0x3684a8007dc9df696a86b0c5c89a8032b78b5b0d'
                        AND block_timestamp > '2022-01-01'
                    )
                    AND block_timestamp > '2022-01-01'
                ),
                t1 AS (
                  SELECT
                    date_trunc('day', v1.block_timestamp) AS dayt,
                    decoded_log:buyer AS user_address
                  FROM
                    metawin_txs v1
                    JOIN ethereum.core.fact_transactions v2 ON v1.tx_hash = v2.tx_hash
                  WHERE
                    v1.event_name = 'EntrySold'
                    and v2.eth_value > 0
                    and v2.block_timestamp > '2022-01-01'
                  GROUP BY
                    1,
                    2
                ),
                t2 AS (
                  SELECT
                    date_trunc('day', t1.dayt) AS dayt,
                    user_address,
                    COUNT(*) as num_days
                  FROM
                    t1
                  GROUP BY
                    1,
                    2
                  HAVING
                    COUNT(*) >= 1
                ),
                active_users AS (
                  SELECT
                    dayt,
                    num_days,
                    COUNT(*) as num_active_users
                  FROM
                    t2
                  GROUP BY
                    1,
                    2
                )
                SELECT
                  dayt as tx_dt,
                  num_active_users,
                  AVG(num_active_users) OVER (
                    ORDER BY
                      dayt
                  ) AS avg_num_active_users
                FROM
                  active_users
                GROUP BY
                  1,
                  2
        """

        # Run the query against Flipside's query engine and await the results
        query_result_set = flipside.query(sql)

        df_users = auto_paginate_result(query_result_set)

        df_users = pd.DataFrame(df_users)

        df_users.to_csv(f"{file_path}_users.csv", ',')

    # Sorting Df values by Date in ascending order
    df_users = df_users.sort_values(by=['tx_dt'], ascending=True)

    # Convert the date column to a datetime format
    df_users['tx_dt'] = pd.to_datetime(df_users['tx_dt']).dt.date

    # Filter the data by time period
    df_users_filtered = metawin_filter_df(df_users, time_period)

    with col1:
        # Average Number of Daily Ticket Buyers (Paid Entries)
        avg_num_daily_ticket_buyers = df_users_filtered["num_active_users"].mean()
        st.write("Average Number of Daily Ticket Buyers (Paid Entries):", avg_num_daily_ticket_buyers)

        # Plot the number of Daily Ticket Buyers (Paid Entries)
        fig = px.bar(
            df_users_filtered,
            x="tx_dt",
            y="num_active_users",
            title="Daily Ticket Buyers (Paid Entries) ({})".format(time_period),
            labels={"tx_dt": "Day", "num_active_users": "Number of Users"}
        )

        st.plotly_chart(fig)

        # Plot the Moving Average Daily Ticket Buyers (Paid Entries)
        fig = px.line(
            df_users_filtered,
            x="tx_dt",
            y="avg_num_active_users",
            title="Moving Average Daily Ticket Buyers (Paid Entries) ({})".format(time_period),
            labels={"tx_dt": "Day", "avg_num_active_users": "Average Number of Users"}
        )

        st.plotly_chart(fig)