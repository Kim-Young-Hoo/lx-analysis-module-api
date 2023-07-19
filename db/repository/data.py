import datetime

from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text
import pandas as pd

from core.hashing import Hasher

from db.models.data import GgsStatis, GgsCmmn
from schemas.data import ShowVariableDetail


def retrieve_variable_list(db: Session):
    result = {}

    depth1_result = db.query(GgsCmmn).filter(GgsCmmn.lclsf_cmmn_cd.like('M01%')).all()
    for row in depth1_result:
        result[row.cmmn_cd] = {
            "cmmn_cd_nm": row.cmmn_cd_nm,
            "children": []
        }

    depth2_result = db.query(GgsCmmn).filter(GgsCmmn.lclsf_cmmn_cd.like('M02%')).all()
    for row in depth2_result:
        result[row.etc_cn_1]["children"].append(
            {
                row.cmmn_cd: {
                    "cmmn_cd_nm": row.cmmn_cd_nm,
                    "etc_cn_2": row.etc_cn_2,
                    "etc_cn_3": row.etc_cn_3,
                    "etc_cn_4": row.etc_cn_4
                }
            }
        )

    return result


def retrieve_variable_detail(id: str, db: Session):
    dummy_result = ShowVariableDetail(
        name="some_name",
        provider="some_name",
        category="some_name",
        unit="some_name",
        recent_upload_date=datetime.date.today(),
        update_period="some_name",
        data_range="some_name"
    )

    return dummy_result


def retrieve_variable_chart_data(id: str, year: str, value_period_type: str, chart_type, db: Session):
    sql_query = text(
        '''
        SELECT
              yr,
              stdg_cd,
              MAX(CASE WHEN dat_no = '{id}' THEN {value_period_type} END) AS v1
        FROM
              ggs_statis
        WHERE
              yr = '{year}'
        GROUP BY
              yr,
              stdg_cd;
        '''.format(id=id, year=year, value_period_type=value_period_type)
    )

    sql_query = text(
        '''
        SELECT
            *
        FROM
            ggs_statis
        WHERE
            yr = '{year}'
        AND dat_no = '{id}'
        '''.format(year=year, id=id)
    )
    db_result = db.execute(sql_query)
    df = pd.DataFrame(db_result.fetchall(), columns=db_result.keys())
    pivoted_df = pd.pivot_table(df, values=value_period_type, index=['yr', 'stdg_cd'], columns='dat_no')
    return db_result
