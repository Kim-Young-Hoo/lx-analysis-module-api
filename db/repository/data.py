import datetime
import os
import uuid

from fastapi import HTTPException
from typing import Literal, List

from numpy import select
from sqlalchemy.orm import Session, aliased
from sqlalchemy import create_engine, text, func, and_, Integer, or_, bindparam, distinct
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

    if period_unit not in data:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="기간 단위 조건이 맞지 않습니다.")

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
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="기간 설정 조건이 맞지 않습니다.")

    return data[period_unit][detail_period]


def get_region_unit_id(region_unit):
    data = {
        "sido": "M040001",
        "sg": "M040002",
        "sgg": "M040003",
        "emd": "M040004",
        "sggemd": "M040005"
    }

    if region_unit not in data[region_unit]:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="잘못된 지역코드입니다.")

    return data.get(region_unit, [])


def get_value_period_list(period_unit: str) -> List[str]:
    if period_unit == "year":
        return ["yr_vl"]
    elif period_unit == "month":
        return ["jan", "feb", "mar", "apr", "may", "jun", "july", "aug", "sep", "oct", "nov", "dec"]
    elif period_unit == "quarter":
        return ["qu_1", "qu_2", "qu_3", "qu_4"]
    elif period_unit == "half":
        return ["ht_1", "ht_2"]


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


def retrieve_variable_chart_data(id: str, year: str, period_unit: str, detail_period, chart_type, db: Session):
    column = get_detail_filter_condition(period_unit, detail_period)

    query_template = """
        select 
            stat.{column}::integer,
            stdg.stdg_nm 
        from 
            ggs_statis stat
        left join 
            ggs_stdg stdg
        on 
            stat.stdg_cd = stdg.stdg_cd 
        where 
            dat_no=:id
        and
            stat.yr=:year
        and stat.{column}::integer is not null
    """.format(column=column)

    params = {
        "year": year,
        "id": id
    }

    query = text(query_template)
    db_result = db.execute(query, params).fetchall()

    if len(db_result) == 0:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="해당 ID의 데이터가 없습니다.")

    dat_nm = db.execute(text("select dat_nm from ggs_data_info gdi where dat_no=:id"), {"id": id}).first()[0]

    if chart_type == "pie":
        return {
            "name": '{}년 {} 파이차트'.format(year, dat_nm),
            "type": 'pie',
            "data": [{"value": ele[0], "name": ele[1]} for ele in db_result]
        }

    elif chart_type == "bar":
        return {
            "name": '{}년 {} 바 차트'.format(year, dat_nm),
            "type": 'bar',
            "data": [{"value": ele[0], "name": ele[1]} for ele in db_result]
        }

    elif chart_type == "histogram":
        histogram_data = get_histogram_data([db_result[i][0] for i in range(len(db_result))])
        return {
            "name": '{}년 {} 히스토그램'.format(year, dat_nm),
            "type": 'bar',
            "data": histogram_data
        }


def get_histogram_data(data):
    data = sorted(data)

    num_bins = 100
    data_min = min(data)
    data_max = max(data)
    bin_width = (data_max - data_min) // num_bins

    bins = [0] * num_bins

    for d in data:
        bin_index = min(int((d - data_min) // bin_width), num_bins - 1)
        bins[bin_index] += 1

    histogram_data = []
    for i in range(num_bins):
        x_position = data_min + (bin_width * i) + int(bin_width / 2)  # Use the midpoint of the bin
        histogram_data.append({
            "x_axis": x_position,
            "count": bins[i]
        })
    return histogram_data


def retrieve_filter_list(db: Session):
    distinct_years = db.query(distinct(GgsStatis.yr)).order_by(GgsStatis.yr).all()
    years = [year[0] for year in distinct_years]

    return {
        "year": years,
        "period_unit": ["year", "half", "quarter", "month"],
        "detail_period": {
            "year": ["all"],
            "half": ["1", "2"],
            "quarter": ["1", "2", "3", "4"],
            "month": ["1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"]
        }
    }


def get_pivoted_df(variable_list: List[str],
                   year: str,
                   period_unit: Literal["year", "month", "quarter", "half"],
                   detail_period: Literal["all", "1", "2", "3", "4", "5", "6", "7", "8", "9", "10", "11", "12"],
                   db: Session
                   ):
    if len(variable_list) > 10:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail="variable list의 최대 개수는 10개입니다.")

    value_period_list = get_detail_filter_condition(period_unit, detail_period)

    query_template = """
        SELECT
            stat.stdg_cd,
            stat.yr,
            stat.dat_no,
            info.dat_nm,
            stdg.stdg_nm,
            stat.jan,
            stat.feb,
            stat.mar,
            stat.apr,
            stat.may,
            stat.jun,
            stat.july,
            stat.aug,
            stat.sep,
            stat.oct,
            stat.nov,
            stat.dec,
            stat.qu_1,
            stat.qu_2,
            stat.qu_3,
            stat.qu_4,
            stat.ht_1,
            stat.ht_2,
            stat.yr_vl
        FROM ggs_statis stat
        JOIN ggs_data_info info ON stat.dat_no = info.dat_no
        JOIN ggs_stdg stdg ON stat.stdg_cd = stdg.stdg_cd 
        WHERE stat.dat_no IN ({})
        AND yr='{}'
    """
    placeholders = ', '.join([':param{}'.format(i) for i in range(len(variable_list))])
    query = text(query_template.format(placeholders, year))
    params = {f'param{i}': value for i, value in enumerate(variable_list)}
    result = db.execute(query, params)

    df = pd.DataFrame(result.fetchall(), columns=result.keys())
    melted_df = pd.melt(df, id_vars=['yr', 'stdg_nm', 'dat_no', 'dat_nm'], value_vars=value_period_list)
    pivoted_df = pd.pivot_table(melted_df, values='value', index=['yr', 'stdg_nm', 'variable'], columns='dat_no')

    dat_no_dat_nm_dict = df.set_index('dat_no')['dat_nm'].to_dict()

    # pivoted_df.to_csv("analysis_module/dataset/data.csv")
    # pivoted_df = pd.read_csv("analysis_module/dataset/data.csv")

    _uuid = uuid.uuid4()

    pivoted_df.to_csv("./data{}.csv".format(_uuid))
    pivoted_df = pd.read_csv("./data{}.csv".format(_uuid))
    os.remove("./data{}.csv".format(_uuid))
    return pivoted_df, dat_no_dat_nm_dict
