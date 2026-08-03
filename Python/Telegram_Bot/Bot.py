from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes, Defaults
from telegram.constants import ParseMode
from telegram.error import NetworkError
from datetime import date
import traceback
import Queries as q
import Billing.Processes.Server_Connections as sc
import Billing.Billing_Data_Migration as bdm
import Billing.Billing_Freeze_Data as bfd
import Billing.Billing_Unfreeze_Data as bud
import Billing.Processes.Billing_Data_Analysis as bda
import Processes.No_Records_Found as nrf
import Billing.Pending_Cases as pc
import Billing.Processes.Scheduler as s
import Billing.Pending_UAD as pu
import Billing.Pending_Freeze as pf
import Billing.Processes.KM_Dashboard as kd

async def error_handler(update, context):
    # Ignore temporary network errors
    if isinstance(context.error, NetworkError):
        return

    print(f"Telegram Error: {context.error}")

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 123456789:
        await update.message.reply_text(q.help_commands(), parse_mode=ParseMode.HTML)

async def migrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if sc.get_status(sc.km_activity_status_file()) == 'Idle':
        try:
            args = context.args or []
            start_date = args[0]
            end_date = args[1]
            if len(args) == 2:
                buffer_minutes = 10
            elif len(args) == 3:
                buffer_minutes = int(args[2])
            else:
                raise
            val_status = q.validations(update.effective_user.id, start_date, end_date)
            if val_status:
                try:
                    await update.message.reply_text('Migration Started...')
                    migration_status = bdm.main(start_date, end_date, buffer_minutes)
                    await update.message.reply_text(migration_status)
                except Exception:
                    await update.message.reply_text('Migration FAILED.')
                    traceback.print_exc()
            else:
                await update.message.reply_text(val_status)
        except Exception:
            await update.message.reply_text('Please provide the command in the following format:- '
                                            '\n /migrate [start date] [end date] [buffer minutes(optional)] \n /migrate 2023-09-15 2023-09-30 20')
    else:
        await update.message.reply_text(sc.running_status())

async def analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        start_date = args[0]
        end_date = args[1]
        if len(args) == 2:
            send_email = 'no'
        elif len(args) == 3:
            send_email = args[2]
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text('Analysis Started...')
                analysis_status = bda.main(start_date, end_date, send_email)
                if analysis_status:
                    await update.message.reply_text('Analysis Completed.')
                else:
                    await update.message.reply_text(analysis_status)
            except Exception:
                await update.message.reply_text('Analysis FAILED.')
                traceback.print_exc()
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /analysis [start date] [end date] [send email(optional,default=''no'')]'
                                        '\n /analysis 2023-09-15 2023-09-30 yes')

async def migana(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if sc.get_status(sc.km_activity_status_file()) == 'Idle':
        try:
            start_date = end_date = context.args[0]
            val_status = q.validations(update.effective_user.id, start_date, end_date)
            if val_status:
                try:
                    await update.message.reply_text('Migration & Analysis Started...')
                    migration_status = bdm.main(start_date, end_date, 10)
                    if migration_status == 'Migration Completed.':
                        analysis_status = bda.main(start_date, end_date, 'no')
                        if analysis_status:
                            await update.message.reply_text('Migration & Analysis Completed.')
                        else:
                            await update.message.reply_text(analysis_status)
                    else:
                        await update.message.reply_text(migration_status)
                except Exception:
                    await update.message.reply_text('Migration & Analysis FAILED.')
                    traceback.print_exc()
            else:
                await update.message.reply_text(val_status)
        except Exception:
            await update.message.reply_text('Please provide the command in the following format:- '
                                            '\n /migana [date]\n /migana 2026-06-15')
    else:
        await update.message.reply_text(sc.running_status())

async def freeze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        today = date.today()
        if len(args) == 3:
            start_date = end_date = args[0]
            last_modified_date = f"{today} {args[1]}:00"
            scope = int(args[2])
        elif len(args) == 4:
            start_date = args[0]
            end_date = args[1]
            last_modified_date = f"{today} {args[2]}:00"
            scope = int(args[3])
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text('Freezing Started...')
                if bfd.main(start_date + ' 00:00:00', end_date + ' 23:59:59', last_modified_date, scope=scope):
                    await update.message.reply_text('Freezing Completed.')
                else:
                    raise
            except Exception:
                await update.message.reply_text('Freezing FAILED.')
                traceback.print_exc()
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /freeze [start date] [end date(optional)] [last modified time] [scope] '
                                        '\n /freeze 2023-09-15 2023-09-20 13:20 1')

async def unfreeze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            reason = context.args[0]
            cases = ''
            for arg in context.args[1:]:
                cases = cases + arg
            if sc.get_status() == 'Idle':
                try:
                    sc.set_status('Busy')
                    await update.message.reply_text('Unfreezing Started...')
                    unfreeze_status = bud.main(reason, cases)
                    await update.message.reply_text(unfreeze_status)
                    if unfreeze_status != 'Cases belong to Freezed Date':
                        nrf_cases = bud.no_record_found(cases)
                        if nrf_cases is not None:
                            await update.message.reply_text('No Record Found Cases\n' + nrf_cases)
                except Exception:
                    await update.message.reply_text('Unfreezing FAILED.')
                    traceback.print_exc()
                finally:
                    sc.set_status('Idle')
            else:
                await update.message.reply_text(sc.running_status())
        except Exception:
            await update.message.reply_text('Please provide the command in the following format:- '
                                            '\n /unfreeze [reason] [cases]'
                                            '\n /unfreeze UAD'
                                            '\n 20232090070459,'
                                            '\n 20232090069968,'
                                            '\n 20231090216369')
    else:
        await update.message.reply_text(val_message)

async def online(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text(q.online())
        except Exception:
            await update.message.reply_text('Online Status FAILED.')
    else:
        await update.message.reply_text(val_message)

async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text(q.sync())
        except Exception:
            await update.message.reply_text('Last Sync Time FAILED.')
    else:
        await update.message.reply_text(val_message)

async def rtnm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        if len(args) == 1:
            start_date = end_date = args[0]
        elif len(args) == 2:
            start_date = args[0]
            end_date = args[1]
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text(q.rtnm(start_date, end_date))
            except Exception:
                await update.message.reply_text('RNTM Status FAILED.')
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                    '\n /rtnm [start date] [end date(optional)] \n /rtnm 2023-09-15 2023-09-30')

async def no_record_found(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            cases = ''
            for arg in context.args[0:]:
                cases = cases + arg
            try:
                await update.message.reply_text('No Record Found Cases Started...')
                await update.message.reply_text(nrf.main('Manual', cases, nrf_email='yes'))
            except Exception:
                await update.message.reply_text('No Record Found Cases FAILED.')
                traceback.print_exc()
        except Exception:
            await update.message.reply_text('Please provide the command in the following format:- '
                                            '\n /nrf [cases]'
                                            '\n /nrf '
                                            '\n 20232090070459,'
                                            '\n 20232090069968,'
                                            '\n 20231090216369')
    else:
        await update.message.reply_text(val_message)

async def kms_activity(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('KMs Activity Unlock Started...')
            await update.message.reply_text(nrf.main(process_type='KMs_Activity', nrf_email=context.args[0].lower()))
            await update.message.reply_text('KMs Activity Unlock Completed.')
        except Exception:
            await update.message.reply_text('KMs Activity Unlock FAILED.')
    else:
        await update.message.reply_text(val_message)

async def scheduler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('Scheduler Started...')
            if s.autofreeze():
                await update.message.reply_text('Scheduler Completed.')
            else:
                raise
        except Exception:
            await update.message.reply_text('Scheduler FAILED.')
            traceback.print_exc()
    else:
        await update.message.reply_text(val_message)

async def post_scheduler(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    # Post Scheduler will always run after Scheduler
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('Post Scheduler Started...')
            s.pcr()
            s.manual_cases_gt_5_percent()
            s.gps_manual_summary()
            s.modified_cases()
            s.cm_dashboard()
            await update.message.reply_text('Post Scheduler Completed.')
        except Exception:
            await update.message.reply_text('Post Scheduler FAILED.')
            traceback.print_exc()
    else:
        await update.message.reply_text(val_message)

async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        if len(args) == 1:
            start_date = end_date = args[0]
            unfreeze_nrf = 'yes'
        elif len(args) == 3:
            start_date = args[0]
            end_date = args[1]
            unfreeze_nrf = args[2]
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text('Pending Cases Started...')
                await update.message.reply_text(pc.main(start_date, end_date, 'yes', unfreeze_nrf))
            except Exception:
                await update.message.reply_text('Pending Cases FAILED.')
                traceback.print_exc()
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /pending [start date] [end date(optional)] [unfreeze_nrf(optional,default=''yes'')] '
                                        '\n /pending 2023-09-16 2023-09-17 no')

async def uad(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        if len(args) == 1:
            start_date = end_date = args[0]
        elif len(args) == 2:
            start_date = args[0]
            end_date = args[1]
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text(pu.main(start_date, end_date, 'Manual'))
            except Exception:
                await update.message.reply_text('Pending UAD Cases FAILED.')
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /uad [start date] [end date(optional)] \n /uad 2023-09-16 2023-09-17')

async def pending_freeze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        args = context.args or []
        if len(args) == 1:
            start_date = end_date = args[0]
        elif len(args) == 2:
            start_date = args[0]
            end_date = args[1]
        else:
            raise
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status:
            try:
                await update.message.reply_text(pf.main(start_date, end_date, 'Yes', 'No', 'Automatic'))
            except Exception:
                await update.message.reply_text('Pending Freeze Cases FAILED.')
        else:
            await update.message.reply_text(val_status)
    except Exception:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /pf [start date] [end date(optional)] \n /pf 2023-09-16 2023-09-17')

async def km_dashboard(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('Data Migration for KM Dashboard Started...')
            if kd.main(0):
                await update.message.reply_text('Data Migration for KM Dashboard Completed.')
            else:
                raise
        except Exception:
            await update.message.reply_text('Data Migration for KM Dashboard FAILED.')
            traceback.print_exc()
    else:
        await update.message.reply_text(val_message)

def main() -> None:
    application = Application.builder().token(sc.decrypt('t_api')).defaults(Defaults(quote=False)).build()
    application.add_error_handler(error_handler)

    application.add_handler(CommandHandler("help", help))
    application.add_handler(CommandHandler("migrate", migrate))
    application.add_handler(CommandHandler("analysis", analysis))
    application.add_handler(CommandHandler("migana", migana))
    application.add_handler(CommandHandler("freeze", freeze))
    application.add_handler(CommandHandler("unfreeze", unfreeze))
    application.add_handler(CommandHandler("online", online))
    application.add_handler(CommandHandler("sync", sync))
    application.add_handler(CommandHandler("rtnm", rtnm))
    application.add_handler(CommandHandler("nrf", no_record_found))
    application.add_handler(CommandHandler("kma", kms_activity))
    application.add_handler(CommandHandler("scheduler", scheduler))
    application.add_handler(CommandHandler("pending", pending))
    application.add_handler(CommandHandler("uad", uad))
    application.add_handler(CommandHandler("pf", pending_freeze))
    application.add_handler(CommandHandler("pscheduler", post_scheduler))
    application.add_handler(CommandHandler("kmd", km_dashboard))

    application.run_polling()

if __name__ == "__main__":
    main()

# 🔴 BOT IS OFFLINE 🔴    🟢 BOT IS ONLINE 🟢
