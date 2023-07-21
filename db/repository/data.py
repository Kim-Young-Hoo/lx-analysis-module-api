import datetime

from numpy import select
from sqlalchemy.orm import Session
from sqlalchemy import create_engine, text, func, and_, Integer
import pandas as pd

from core.hashing import Hasher

from db.models.data import GgsStatis, GgsCmmn, GgsDataInfo
from schemas.data import ShowVariableDetail


def retrieve_variable_list(year, db: Session):
    result = {}

    depth1_result = db.query(GgsCmmn).filter(
        GgsCmmn.cmmn_cd.like('M01%'),
        GgsCmmn.use_yn == "Y"
    ).order_by(GgsCmmn.indct_orr).all()

    for row in depth1_result:
        result[row.cmmn_cd] = {
            "name": row.cmmn_cd_nm,
            "order_index": int(row.indct_orr),
            "children": []
        }

    depth2_result = db.query(GgsDataInfo).filter(
        GgsDataInfo.dat_no.like('M0002%'),
        and_(
            GgsDataInfo.dat_scop_bgng.cast(Integer) <= year,
            GgsDataInfo.dat_scop_end.cast(Integer) >= year
        ),
        GgsDataInfo.use_yn == "Y"
    ).all()

    for row in depth2_result:
        result[row.clsf_cd]["children"].append(
            {
                row.dat_no: {
                    "name": row.dat_nm,
                    "order_index": row.indct_orr,
                    "children": []
                }
            }
        )

    return result


def retrieve_variable_detail(id: str, db: Session):
    query_template = """
        select
            a.dat_no,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.CLSF_CD = a1.cmmn_cd) as clsf_nm,
            a.dat_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.RGN_SE = a1.cmmn_cd) as rgn_nm,
            (select  a1.cmmn_cd_nm from ggs_cmmn a1 where a.PD_SE = a1.cmmn_cd) as pd_nm,
            a.REL_DAT_LIST_NM,
            a.REL_TBL_NM,   
            a.REL_FILD_NM,
            a.DAT_SRC,
            a.UPDT_CYLE,
            a.DAT_SCOP_BGNG,
            a.DAT_SCOP_END,
            a.last_mdfcn_dt
        from GGS_DATA_INFO a
        where
            USE_YN = 'Y'
            and dat_no='{id}'
        order by a.dat_no;
    """.format(id=id)

    query = db.execute(text(query_template))
    return query.first()


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
