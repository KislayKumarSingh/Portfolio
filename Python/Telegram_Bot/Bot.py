from telegram import Update
from telegram.ext import Application, CommandHandler, ContextTypes
from telegram.constants import ParseMode
import Queries as q
import Billing.Processes.Server_Connections as sc
import Billing.Billing_Data_Migration as bdm
import Billing.Billing_Freeze_Data as bfd
import Billing.Billing_Unfreeze_Data as bud
import Reports.KPI.KPI_Govind as kg
import Billing.Billing_Data_Analysis as bda
import Billing.No_Records_Found as nrf
import Billing.Pending_Cases as pc
import Billing.Processes.Scheduler as s
import Billing.Pending_UAD as pu
import Billing.Billing_Data_Missing_Cases as bdmc
import Billing.Processes.Delete_KMs_Activity as dka

async def help(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    if update.effective_user.id == 1234567890:
        await update.message.reply_text(q.help_commands(), parse_mode=ParseMode.HTML)

async def migrate(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            if len(context.args) == 2:
                buffer_minutes = 10
            else:
                buffer_minutes = int(context.args[2])
            try:
                await update.message.reply_text('Migration Started...')
                migration_status = bdm.main(start_date, end_date, buffer_minutes)
                await update.message.reply_text(migration_status)
            except:
                await update.message.reply_text('Migration FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /migrate [start date] [end date] [buffer minutes(optional)] \n /migrate 2023-09-15 2023-09-30 20')

async def freeze(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            last_modified_date = context.args[2] + ' ' + context.args[3]
            try:
                if len(context.args) == 4:
                    scope = 0
                else:
                    scope = int(context.args[4])
                await update.message.reply_text('Freezing Started...')
                bfd.main(start_date + ' 00:00:00', end_date + ' 23:59:59', last_modified_date, scope=scope)
                await update.message.reply_text('Freezing Completed.')
            except:
                await update.message.reply_text('Freezing FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /freeze [start date] [end date] [last modified date] \n /freeze 2023-09-15 2023-09-20 2023-09-20 13:00:00')

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
                    await update.message.reply_text(bud.main(reason, cases))
                    nrf_cases = bud.no_record_found(cases)
                    if nrf_cases is not None:
                        await update.message.reply_text('No Record Found Cases\n' + nrf_cases)
                    await update.message.reply_text('Unfreezing Completed.')
                except:
                    await update.message.reply_text('Unfreezing FAILED.')
                finally:
                    sc.set_status('Idle')
            else:
                await update.message.reply_text(sc.running_status())
        except:
            await update.message.reply_text('Please provide the command in the following format:- '
                                            '\n /unfreeze [reason] [cases]'
                                            '\n /unfreeze UAD'
                                            '\n 20232090070459,'
                                            '\n 20232090069968,'
                                            '\n 20231090216369')
    else:
        await update.message.reply_text(val_message)

async def kpi(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('KPI Started...')
                kg.main(start_date, end_date)
                await update.message.reply_text('KPI Completed.')
            except:
                await update.message.reply_text('KPI FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /kpi_govind [start date] [end date] \n /kpi_govind 2023-09-15 2023-09-30')

async def online(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('Online Status Started...')
            await update.message.reply_text(q.online())
            await update.message.reply_text('Online Status Completed.')
        except:
            await update.message.reply_text('Online Status FAILED.')
    else:
        await update.message.reply_text(val_message)

async def sync(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            await update.message.reply_text('Last Sync Time Started...')
            await update.message.reply_text(q.sync())
            await update.message.reply_text('Last Sync Time Completed.')
        except:
            await update.message.reply_text('Last Sync Time FAILED.')
    else:
        await update.message.reply_text(val_message)

async def analysis(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        send_email = context.args[2]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('Analysis Started...')
                bda.main(start_date, end_date, send_email)
                await update.message.reply_text('Analysis Completed.')
            except:
                await update.message.reply_text('Analysis FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /analysis [start date] [end date] [send email]\n /analysis 2023-09-15 2023-09-30 Yes')

async def rtnm(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('RTNM Status Started...')
                await update.message.reply_text(q.rtnm(start_date, end_date))
                await update.message.reply_text('RTNM Status Completed.')
            except:
                await update.message.reply_text('RNTM Status FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                    '\n /rtnm [start date] [end date] \n /rtnm 2023-09-15 2023-09-30')

async def no_record_found(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    val_status, val_message = q.user_time_validation(update.effective_user.id)
    if val_status:
        try:
            cases = ''
            for arg in context.args[0:]:
                cases = cases + arg
            try:
                await update.message.reply_text('No Record Found Cases Started...')
                await update.message.reply_text(nrf.main('Manual', cases))
                await update.message.reply_text('No Record Found Cases Completed.')
            except:
                await update.message.reply_text('No Record Found Cases FAILED.')
        except:
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
        except:
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
        except:
            await update.message.reply_text('Scheduler FAILED.')
    else:
        await update.message.reply_text(val_message)

async def pending(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            unfreeze_nrf = 'no'
            if len(context.args) > 2:
                if context.args[2].lower() == 'yes':
                    unfreeze_nrf = 'yes'
            try:
                await update.message.reply_text('Pending Cases Started...')
                await update.message.reply_text(pc.main(start_date, end_date, 'yes', unfreeze_nrf))
                await update.message.reply_text('Pending Cases Completed.')
            except:
                await update.message.reply_text('Pending Cases FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /pending [start date] [end date] [unfreeze_nrf(optional)] \n /pending 2023-09-16 2023-09-17 no')

async def uad(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('Pending UAD Cases Started...')
                await update.message.reply_text(pu.main(start_date, end_date, 'Manual'))
                await update.message.reply_text('Pending UAD Cases Completed.')
            except:
                await update.message.reply_text('Pending UAD Cases FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /uad [start date] [end date] \n /uad 2023-09-16 2023-09-17')

async def missing(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('Missing Cases Started...')
                await update.message.reply_text(bdmc.main(start_date, end_date, 'Yes', 'No', 'Automatic'))
                await update.message.reply_text('Missing Cases Completed.')
            except:
                await update.message.reply_text('Missing Cases FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /missing [start date] [end date] \n /missing 2023-09-16 2023-09-17')

async def kmd(update: Update, context: ContextTypes.DEFAULT_TYPE) -> None:
    try:
        start_date = context.args[0]
        end_date = context.args[1]
        val_status = q.validations(update.effective_user.id, start_date, end_date)
        if val_status == True:
            try:
                await update.message.reply_text('Delete KMs Activity Started...')
                await update.message.reply_text(dka.main(start_date, end_date))
                await update.message.reply_text('Delete KMs Activity Completed.')
            except:
                await update.message.reply_text('Delete KMs Activity FAILED.')
        else:
            await update.message.reply_text(val_status)
    except:
        await update.message.reply_text('Please provide the command in the following format:- '
                                        '\n /kmd [start date] [end date] \n /kmd 2023-09-16 2023-09-17')

def main() -> None:
    # Create the Application and pass it your bot's token.
    application = Application.builder().token(sc.telegram_api).build()

    # on different commands - answer in Telegram
    application.add_handler(CommandHandler("help", help))
    application.add_handler(CommandHandler("migrate", migrate))
    application.add_handler(CommandHandler("freeze", freeze))
    application.add_handler(CommandHandler("unfreeze", unfreeze))
    application.add_handler(CommandHandler("online", online))
    application.add_handler(CommandHandler("sync", sync))
    application.add_handler(CommandHandler("analysis", analysis))
    application.add_handler(CommandHandler("rtnm", rtnm))
    application.add_handler(CommandHandler("kpi_govind", kpi))
    application.add_handler(CommandHandler("nrf", no_record_found))
    application.add_handler(CommandHandler("kma", kms_activity))
    application.add_handler(CommandHandler("scheduler", scheduler))
    application.add_handler(CommandHandler("pending", pending))
    application.add_handler(CommandHandler("uad", uad))
    application.add_handler(CommandHandler("missing", missing))
    application.add_handler(CommandHandler("kmd", kmd))

    # Run the bot until the user presses Ctrl-C
    application.run_polling()

if __name__ == "__main__":
    main()