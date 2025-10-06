# main.py
import argparse, sys
from utils.common import parse_changed_dates
from utils.bronze_ingest import ingest_all
from utils.silver_loans import build_loans_silver
from utils.silver_financials import build_financials_scd2
from utils.silver_attributes import build_attributes_current
from utils.silver_clickstream import build_click_agg
from utils.gold_build_loan_app_mob6 import build_loan_app_mob6
from utils.build_feature_store import build_feature_store
from utils.build_label_store import build_label_store

def run_bronze(args):
    ingest_all(parse_changed_dates(args.changed_dates))

def run_silver(args):
    changed = parse_changed_dates(args.changed_dates)
    # If no flags, run all silver steps
    run_all = not any([args.loans, args.financials, args.attributes, args.clicks])
    if args.loans or run_all:
        build_loans_silver(changed_dates=changed)
    if args.financials or run_all:
        build_financials_scd2(changed_dates=changed)
    if args.attributes or run_all:
        build_attributes_current()
    if args.clicks or run_all:
        build_click_agg(changed_dates=changed)

def run_gold_loan_app(_args):
    build_loan_app_mob6()

def run_feature_store_cmd(_args):
    build_feature_store()

def run_label_store_cmd(_args):
    build_label_store()

def run_all(_args):
    ingest_all(None)
    build_loans_silver(None)
    build_financials_scd2(None)
    build_attributes_current()
    build_click_agg(None)
    build_loan_app_mob6()
    build_feature_store()
    build_label_store()

def make_parser():
    p = argparse.ArgumentParser(description="Underwriting Medallion Pipeline")
    sub = p.add_subparsers(dest="cmd")
    p.set_defaults(func=run_all)  # default to 'all' if no subcommand

    # bronze
    sp_b = sub.add_parser("bronze", help="Ingest CSVs to Bronze")
    sp_b.add_argument("--changed_dates", type=str, default=None, help="Comma-separated YYYY-MM-DD")
    sp_b.set_defaults(func=run_bronze)

    # silver
    sp_s = sub.add_parser("silver", help="Build Silver tables")
    sp_s.add_argument("--changed_dates", type=str, default=None)
    sp_s.add_argument("--loans", action="store_true")
    sp_s.add_argument("--financials", action="store_true")
    sp_s.add_argument("--attributes", action="store_true")
    sp_s.add_argument("--clicks", action="store_true")
    sp_s.set_defaults(func=run_silver)

    # gold
    sp_g = sub.add_parser("gold-loan-app", help="Build loan_app_mob6 (underwriting Gold)")
    sp_g.set_defaults(func=run_gold_loan_app)

    # stores
    sp_fs = sub.add_parser("feature-store", help="Build offline feature store")
    sp_fs.set_defaults(func=run_feature_store_cmd)

    sp_ls = sub.add_parser("label-store", help="Build label store")
    sp_ls.set_defaults(func=run_label_store_cmd)

    # all
    sp_all = sub.add_parser("all", help="Run everything")
    sp_all.set_defaults(func=run_all)

    return p

def main(argv=None):
    argv = argv or sys.argv[1:]
    parser = make_parser()
    args = parser.parse_args(argv)
    args.func(args)

if __name__ == "__main__":
    main()



    