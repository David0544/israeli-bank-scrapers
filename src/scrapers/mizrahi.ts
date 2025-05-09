import moment from 'moment';
import type { Moment } from 'moment';
import { type Frame, type HTTPRequest, type Page } from 'puppeteer';
import { SHEKEL_CURRENCY } from '../constants';
import {
  pageEvalAll, waitUntilElementDisappear, waitUntilElementFound, waitUntilIframeFound,
} from '../helpers/elements-interactions';
import { fetchPostWithinPage } from '../helpers/fetch';
import { waitForUrl } from '../helpers/navigation';
import {
  type Transaction,
  TransactionStatuses, TransactionTypes,
  type TransactionsAccount,
} from '../transactions';
import { BaseScraperWithBrowser, LoginResults, type PossibleLoginResults } from './base-scraper-with-browser';
import { ScraperErrorTypes } from './errors';

interface ScrapedTransaction {
  RecTypeSpecified: boolean;
  MC02PeulaTaaEZ: string;
  MC02SchumEZ: number;
  MC02AsmahtaMekoritEZ: string;
  MC02TnuaTeurEZ: string;
  MC02KodGoremEZ: string;
  MC02SeifMaralEZ: string;
  MC02AgidEZ: string;
  MC02SugTnuaKaspitEZ: string;
  MC02ErehTaaEZ: string;
  TransactionNumber: string;
  MC02ShowDetailsEZ: string;
  MC02OfiTnuaEZ: string;
  SkyChequeIndex: number;
}
interface ScrapedTransactionsResult {
  header: {
    success: boolean;
    messages: { text: string }[];
  };
  body: {
    fields: {
      Yitra: string;
    };
    table: {
      rows: ScrapedTransaction[];
    };
  };
}

interface TransactionDetailsIn {
  inKodGorem: string;
  inAsmachta: string;
  inSchum: number;
  inNakvanit: string;
  inSugTnua: string;
  inAgid: string;
  inTarPeulaFormatted: string;
  inTarErechFormatted: string;
  inKodNose: string;
  inKodTatNose: string;
  inTransactionNumber: string;
}

interface TransactionChequesOfDepositIn {
  ServiceName: string;
  ChequeIndex: number;
  SkyChequeIndex: number;
  DepInx: number;
}

const BASE_WEBSITE_URL = 'https://www.mizrahi-tefahot.co.il';
const LOGIN_URL = `${BASE_WEBSITE_URL}/login/index.html#/auth-page-he`;
const BASE_APP_URL = 'https://mto.mizrahi-tefahot.co.il';
const AFTER_LOGIN_BASE_URL = /https:\/\/mto\.mizrahi-tefahot\.co\.il\/OnlineApp\/.*/;
const OSH_PAGE = '/osh/legacy/legacy-Osh-Main';
const TRANSACTIONS_PAGE = '/osh/legacy/root-main-osh-p428New';
const TRANSACTIONS_REQUEST_URLS = [
  `${BASE_APP_URL}/OnlinePilot/api/SkyOSH/get428Index`,
  `${BASE_APP_URL}/Online/api/SkyOSH/get428Index`,
];
const PENDING_TRANSACTIONS_PAGE = '/osh/legacy/legacy-Osh-p420';
const PENDING_TRANSACTIONS_IFRAME = 'p420.aspx';
const CHANGE_PASSWORD_URL = /https:\/\/www\.mizrahi-tefahot\.co\.il\/login\/index\.html#\/change-pass/;
const DATE_FORMAT = 'DD/MM/YYYY';
const MAX_ROWS_PER_REQUEST = 10000000000;

const usernameSelector = '#emailDesktopHeb';
const passwordSelector = '#passwordIDDesktopHEB';
const submitButtonSelector = '.form-desktop button';
const invalidPasswordSelector = 'a[href*="https://sc.mizrahi-tefahot.co.il/SCServices/SC/P010.aspx"]';
const afterLoginSelector = '#dropdownBasic';
const loginSpinnerSelector = 'div.ngx-overlay.loading-foreground';
const accountDropDownItemSelector = '#AccountPicker .item';
const pendingTrxIdentifierId = '#ctl00_ContentPlaceHolder2_panel1';
const checkingAccountTabHebrewName = 'עובר ושב';
const checkingAccountTabEnglishName = 'Checking Account';

function createLoginFields(credentials: ScraperSpecificCredentials) {
  return [
    { selector: usernameSelector, value: credentials.username },
    { selector: passwordSelector, value: credentials.password },
  ];
}

async function isLoggedIn(options: { page?: Page | undefined } | undefined) {
  if (!options?.page) {
    return false;
  }
  const oshXPath = `//a//span[contains(., "${checkingAccountTabHebrewName}") or contains(., "${checkingAccountTabEnglishName}")]`;
  const oshTab = await options.page.$$(`xpath${oshXPath}`);
  return oshTab.length > 0;
}

function getPossibleLoginResults(page: Page): PossibleLoginResults {
  return {
    [LoginResults.Success]: [AFTER_LOGIN_BASE_URL, isLoggedIn],
    [LoginResults.InvalidPassword]: [async () => !!(await page.$(invalidPasswordSelector))],
    [LoginResults.ChangePassword]: [CHANGE_PASSWORD_URL],
  };
}

function getStartMoment(optionsStartDate: Date) {
  const defaultStartMoment = moment().subtract(1, 'years');
  const startDate = optionsStartDate || defaultStartMoment.toDate();
  return moment.max(defaultStartMoment, moment(startDate));
}

function createDataFromRequest(request: HTTPRequest, optionsStartDate: Date) {
  const data = JSON.parse(request.postData() || '{}');

  data.inFromDate = getStartMoment(optionsStartDate).format(DATE_FORMAT);
  data.inToDate = moment().format(DATE_FORMAT);
  data.table.maxRow = MAX_ROWS_PER_REQUEST;

  return data;
}

function createHeadersFromRequest(request: HTTPRequest) {
  return {
    'mizrahixsrftoken': request.headers().mizrahixsrftoken,
    'Content-Type': request.headers()['content-type'],
  };
}

function convertTransaction(txn: ScrapedTransaction, date: Moment, memo: string | undefined): Transaction {
  const txnDate = date.toISOString();

  return {
    type: TransactionTypes.Normal,
    identifier: txn.MC02AsmahtaMekoritEZ ? parseInt(txn.MC02AsmahtaMekoritEZ, 10) : undefined,
    date: txnDate,
    processedDate: txnDate,
    originalAmount: txn.MC02SchumEZ,
    originalCurrency: SHEKEL_CURRENCY,
    chargedAmount: txn.MC02SchumEZ,
    description: txn.MC02TnuaTeurEZ,
    status: TransactionStatuses.Completed,
    memo,
  };
}

async function extractPendingTransactions(page: Frame): Promise<Transaction[]> {
  const pendingTxn = await pageEvalAll(page, 'tr.rgRow', [], (trs) => {
    return trs.map((tr) => Array.from(tr.querySelectorAll('td'), (td: HTMLTableDataCellElement) => td.textContent || ''));
  });

  return pendingTxn.map((txn) => {
    const date = moment(txn[0], 'DD/MM/YY').toISOString();
    const amount = parseInt(txn[3], 10);
    return {
      type: TransactionTypes.Normal,
      date,
      processedDate: date,
      originalAmount: amount,
      originalCurrency: SHEKEL_CURRENCY,
      chargedAmount: amount,
      description: txn[1],
      status: TransactionStatuses.Pending,
    };
  });
}

async function postLogin(page: Page) {
  await Promise.race([
    waitUntilElementFound(page, afterLoginSelector),
    waitUntilElementFound(page, invalidPasswordSelector),
    waitForUrl(page, CHANGE_PASSWORD_URL),
  ]);
}

type ScraperSpecificCredentials = { username: string, password: string };

class MizrahiScraper extends BaseScraperWithBrowser<ScraperSpecificCredentials> {
  getLoginOptions(credentials: ScraperSpecificCredentials) {
    return {
      loginUrl: LOGIN_URL,
      fields: createLoginFields(credentials),
      submitButtonSelector,
      checkReadiness: async () => waitUntilElementDisappear(this.page, loginSpinnerSelector),
      postAction: async () => postLogin(this.page),
      possibleResults: getPossibleLoginResults(this.page),
    };
  }

  async fetchData() {
    await this.page.$eval('#dropdownBasic, .item', (el) => (el as HTMLElement).click());

    const numOfAccounts = (await this.page.$$(accountDropDownItemSelector)).length;

    try {
      const results: TransactionsAccount[] = [];

      for (let i = 0; i < numOfAccounts; i += 1) {
        if (i > 0) {
          await this.page.$eval('#dropdownBasic, .item', (el) => (el as HTMLElement).click());
        }

        await this.page.$eval(`${accountDropDownItemSelector}:nth-child(${i + 1})`, (el) => (el as HTMLElement).click());
        results.push((await this.fetchAccount()));
      }

      return {
        success: true,
        accounts: results,
      };
    } catch (e) {
      return {
        success: false,
        errorType: ScraperErrorTypes.Generic,
        errorMessage: (e as Error).message,
      };
    }
  }

  private async getPendingTransactions(): Promise<Transaction[]> {
    await this.page.$eval(`a[href*="${PENDING_TRANSACTIONS_PAGE}"]`, (el) => (el as HTMLElement).click());
    const frame = await waitUntilIframeFound(this.page, (f) => f.url().includes(PENDING_TRANSACTIONS_IFRAME));
    const isPending = await waitUntilElementFound(frame, pendingTrxIdentifierId).then(() => true).catch(() => false);
    if (!isPending) {
      return [];
    }

    const pendingTxn = await extractPendingTransactions(frame);
    return pendingTxn;
  }

  private async getDepositDetails(row: ScrapedTransaction, extraHeaders: Record<string, any>)
    : Promise<string | undefined> {
    try {
      const url = `${BASE_APP_URL}/Online/api/OSH/getMaherBerurimSMF`;
      const data: TransactionDetailsIn = {
        inAgid: row.MC02AgidEZ,
        inAsmachta: row.MC02AsmahtaMekoritEZ,
        inKodGorem: row.MC02KodGoremEZ,
        inKodNose: row.MC02SeifMaralEZ,
        inKodTatNose: '0',
        inNakvanit: row.MC02KodGoremEZ,
        inSchum: row.MC02SchumEZ,
        inSugTnua: row.MC02SugTnuaKaspitEZ,
        inTarErechFormatted: moment(row.MC02ErehTaaEZ == '0001-01-01T00:00:00' ? row.MC02PeulaTaaEZ : row.MC02ErehTaaEZ).format(DATE_FORMAT),
        inTarPeulaFormatted:  moment(row.MC02PeulaTaaEZ).format(DATE_FORMAT),
        inTransactionNumber: row.TransactionNumber,
      };

      const extraTransactionDetails = await fetchPostWithinPage<any>(this.page, url, data, extraHeaders);
      if (extraTransactionDetails?.body.fields?.[0]?.[0]?.Records?.[0]?.Fields?.[0]) {
        return JSON.stringify(extraTransactionDetails.body.fields[0][0].Records[0].Fields);
      }
    } catch (e) {
      console.error('Error fetching deposit details', e);
    }

    return undefined;
  }

  private async getDepositShaquesDetails(row: ScrapedTransaction, chequeIndex: number,
    extraHeaders: Record<string, any>) : Promise<string | undefined> {
    const url = `${BASE_APP_URL}/Online/api/OSH/getOshChequesList`;

    let extraTransactionDetails = null;
    let data: TransactionChequesOfDepositIn | null = null;
    try {
      data = {
        SkyChequeIndex: +row.SkyChequeIndex,
        ChequeIndex: chequeIndex,
        DepInx: 0,
        ServiceName: 'GetOutChequesOfDeposit',
      };

      extraTransactionDetails = await fetchPostWithinPage<any>(this.page, url, data, extraHeaders);
      if (extraTransactionDetails?.body?.table?.rows?.[0]) {
        return JSON.stringify(extraTransactionDetails.body.table.rows);
      } else {
        console.error(`Error fetching cheque details: ${extraTransactionDetails.header?.messages?.[0]?.text}`);
      } 
    } catch (e) {
      console.error('Error fetching cheque details. Exception:', e);
    }

    return undefined;
  }

  private async fetchAccount() {
    await this.page.waitForSelector(`a[href*="${OSH_PAGE}"]`);
    await this.page.$eval(`a[href*="${OSH_PAGE}"]`, (el) => (el as HTMLElement).click());
    await waitUntilElementFound(this.page, `a[href*="${TRANSACTIONS_PAGE}"]`);
    await this.page.$eval(`a[href*="${TRANSACTIONS_PAGE}"]`, (el) => (el as HTMLElement).click());

    const accountNumberElement = await this.page.$('#dropdownBasic b span');
    const accountNumberHandle = await accountNumberElement?.getProperty('title');
    const accountNumber = ((await accountNumberHandle?.jsonValue()) as string);
    if (!accountNumber) {
      throw new Error('Account number not found');
    }

    const ret = await Promise.any(TRANSACTIONS_REQUEST_URLS.map(async (url) => {
      const request = await this.page.waitForRequest(url);
      const data = createDataFromRequest(request, this.options.startDate);
      const headers = createHeadersFromRequest(request);

      const response = await fetchPostWithinPage<ScrapedTransactionsResult>(this.page, url, data, headers);

      if (!response || response.header.success === false) {
        return {
          error: response?.header?.messages?.[0].text ?? 'unknown error',
        };
      }
      
      return {
        error: undefined,
        rows: response.body.table.rows,
        yitra: response.body.fields?.Yitra,
        reqHeaders: headers,
      };

    }));

    if (ret.error !== undefined) {
      throw new Error(`Error fetching transaction. Response message: ${ret.error}`);
    }

    const transactions: Transaction[] = [];

    const startMoment = getStartMoment(this.options.startDate);
    let chequeIndex = 0;
    
    for (const row of ret.rows) {
      if (!row.RecTypeSpecified) {
        continue;
      }
    
      // workaround for a bug which the bank's API returns transactions before the requested start date
      const txnDate = moment(row.MC02PeulaTaaEZ, moment.HTML5_FMT.DATETIME_LOCAL_SECONDS);
      if (txnDate.isBefore(startMoment)) {
        continue;
      }

      let memo = undefined;
      const isChequeDeposit = '03' === row.MC02OfiTnuaEZ;
      const isChequeOperation = isChequeDeposit || '01' === row.MC02OfiTnuaEZ || '05' === row.MC02OfiTnuaEZ || '06' === row.MC02OfiTnuaEZ || '07' === row.MC02OfiTnuaEZ || '08' === row.MC02OfiTnuaEZ;      
      const isMoneyTransfer = row.MC02ShowDetailsEZ == '1';

      if (isMoneyTransfer) {
        memo = await this.getDepositDetails(row, ret.reqHeaders);
      } else if (isChequeOperation) {
        if (isChequeDeposit) {
          memo = await this.getDepositShaquesDetails(row, chequeIndex, ret.reqHeaders);
        }
        chequeIndex++;
      }
      
      transactions.push(convertTransaction(row, txnDate, memo));
    }

    const pendingTxn = await this.getPendingTransactions();
    const allTxn = transactions.concat(pendingTxn);

    return {
      accountNumber,
      txns: allTxn,
      balance: +ret?.yitra,
    };
  }
}

export default MizrahiScraper;
