# transform_activation.py
def calculate_activation_rate(data):
    for row in data:
        row["activation_rate"] = row["active_users"] / row["new_users"] * 100
    return data

