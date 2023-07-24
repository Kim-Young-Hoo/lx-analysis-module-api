import datetime
from fastapi import HTTPException
from typing import Literal

from numpy import select
from sqlalchemy.orm import Session, aliased
from sqlalchemy import create_engine, text, func, and_, Integer, or_, bindparam
import pandas as pd
from starlette import status

from core.hashing import Hasher

from db.models.data import GgsStatis, GgsCmmn, GgsDataInfo
from schemas.data import ShowVariableDetail


def get_period_unit_list(period_unit):
    data = {
        "year": ["M030001", "M030002", "M030003", "M030004"],
        "half": ["M030001", "M030002", "M030003"],
        "quarter": ["M030001", "M030002"],
        "month": ["M030001"]
    }
    return data.get(period_unit, [])


def get_detail_filter_condition(period_unit, detail_period):
    data = {
        "year": {"all": "yr_vl"},
        "month": {"1": "jan", "2": "feb", "3": "mar", "4": "apr", "5": "may", "6": "jun", "7": "july", "8": "aug",
                  "9": "sep", "10": "oct", "11": "nov", "12": "dec"},
        "quarter": {"1": "qu_1", "2": "qu_2", "3": "qu_3", "4": "qu_4"},
        "half": {"1": "ht_1", "2": "ht_2"}
    }

    if detail_period not in data[period_unit]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="데이터 조건이 맞지 않습니다.")

    return data[period_unit][detail_period]


def get_region_unit_id(region_unit):
    data = {
        "sido": "M040001",
        "sg": "M040002",
        "sgg": "M040003",
        "emd": "M040004",
        "sggemd": "M040005"
    }
    return data.get(region_unit, [])


def retrieve_variable_list(year: str,
                           region: Literal["all", "gsbd"],
                           period_unit: Literal["year", "month", "quarter", "half"],
                           detail_period: Literal["all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
                           db: Session):
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

    period_unit_list = get_period_unit_list(period_unit)

    depth2_query_template = """
        select 
            distinct gdi.dat_no, gdi.dat_nm, gdi.clsf_cd, gdi.indct_orr, gc.cmmn_cd_nm rgn_se_nm        
        from 
            ggs_data_info gdi
        left join 
            ggs_statis gs
        on 
            gdi.dat_no = gs.dat_no
        left join 
            ggs_cmmn gc
        on 
            gdi.rgn_se = gc.cmmn_cd 
        where yr=:year
        AND (
            (dat_src != '경상북도' AND :region = 'all')
            OR (dat_src = '경상북도' AND :region = 'gb')
        )
        and pd_se in :period_unit_list
        and :detail_period is not null
    """
    depth2_query = text(depth2_query_template)

    depth2_query = depth2_query.bindparams(bindparam('region', expanding=False))
    depth2_query = depth2_query.bindparams(bindparam('period_unit_list', expanding=True))

    depth2_result = db.execute(depth2_query, {'year': year,
                                              'region': region,
                                              'period_unit_list': period_unit_list,
                                              'detail_period': get_detail_filter_condition(period_unit, detail_period)
                                              })

    for row in depth2_result:
        result[row.clsf_cd]["children"].append(
            {
                row.dat_no: {
                    "name": row.dat_nm,
                    "order_index": row.indct_orr,
                    "region_unit": row.rgn_se_nm
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


def retrieve_variable_chart_data(id: str, year: str, period_unit: str, chart_type, db: Session):
    # sql_query = text(
    #     '''
    #     SELECT
    #         *
    #     FROM
    #         ggs_statis
    #     WHERE
    #         yr = '{year}'
    #     AND dat_no = '{id}'
    #     '''.format(year=year, id=id, )
    # )
    # db_result = db.execute(sql_query)
    # df = pd.DataFrame(db_result.fetchall(), columns=db_result.keys())

    if chart_type == "pie":
        return {
            "name": 'dummy pie chart',
            "type": 'pie',
            "data": [
                {"value": 108, "name": 'dummy1'},
                {"value": 735, "name": 'dummy2'},
                {"value": 580, "name": 'dummy3'},
                {"value": 484, "name": 'dummy4'},
                {"value": 300, "name": 'dummy5'}
            ]
        }

    elif chart_type == "bar":
        return {
            "name": 'dummy pie chart',
            "type": 'pie',
            "data": [
                {"value": 108, "name": 'dummy1'},
                {"value": 735, "name": 'dummy2'},
                {"value": 580, "name": 'dummy3'},
                {"value": 484, "name": 'dummy4'},
                {"value": 300, "name": 'dummy5'}
            ]
        }

    elif chart_type == "histogram":
        return {
            "name": 'dummy histogram',
            "type": 'bar',
            "data": [
                {"value": 20, "x_axis": 5},
                {"value": 52, "x_axis": 15},
                {"value": 200, "x_axis": 25},
                {"value": 334, "x_axis": 35},
                {"value": 390, "x_axis": 45},
                {"value": 330, "x_axis": 55},
                {"value": 220, "x_axis": 65},
            ]

        }

    # return db_result
