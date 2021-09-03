import requests
import pandas as pd

from configuration import github_config


def get_commits_table(repo: str, path: str, page: int) -> pd.DataFrame:
    """
    Collects all commit ids for a specific file and returns a dataframe containing the commit ids and
    the corresponding date.
    :param repo: "italia/covid19-opendata-vaccini" or "napo/covid19apssdashboard"
    :param path: "dati/consegne-vaccini-latest.csv" or "data/stato_comuni_td.csv"
    :param page: integer referring to the number of page
    :return: pd.DataFrame with date and commit ids
    """
    assert github_config.TOKEN is not None, "Github token was not provided"
    res = requests.get(f"https://api.github.com/repos/{repo}/commits?state=closed&access_token={github_config.TOKEN} \
    &path={path}&per_page=100&page={page}")
    assert res.status_code == 200, f"Got status code {res.status_code} from GitHub API when requesting commits table"

    elements = res.json()

    commits_df = pd.DataFrame(columns=["date", "commit_id"])

    for el in elements:
        try:
            date = pd.to_datetime(el["commit"]["committer"]["date"]).date()
        except TypeError:
            assert True, "Could not extract date of commit"
        commit_id = el["sha"]
        if date not in commits_df['date'].values:
            commits_df = commits_df.append({"date": date, "commit_id": commit_id}, ignore_index=True)

    return commits_df


def get_data_version(repo: str, path: str, commit_id: str):
    """
    Gets data associated with a specific commit id.
    :param repo: "italia/covid19-opendata-vaccini" or "napo/covid19apssdashboard"
    :param path: "dati/consegne-vaccini-latest.csv" or "data/stato_comuni_td.csv"
    :param commit_id: one commit id
    :return:
    """
    data_df = pd.read_csv(
        f"https://raw.githubusercontent.com/{repo}/{commit_id}/{path}")
    return data_df
