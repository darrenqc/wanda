
const fs = require('fs');
const Crawler = require('crawler');
const moment = require('moment');
const logger = require('bda-util/winston-rotate-local-timezone').getLogger(`./log/wanda.log`);
const date = moment().format('YYYY_MM_DD');
const today = moment().format('YYYY-MM-DD');
const RETRY_ON_ERROR_PER_CINEMA = 20;
//const STOP_CRITERIA_SECONDS_BEFORE_SHOWTIME = 120;
const STOP_CRITERIA_SECONDS_BEFORE_SHOWTIME = parseInt(process.argv.splice(2)[0]);

const ProxyManager = {
    proxies:require('./appdata/proxies.json'),
    idx:0,
    getProxy: function(){
	let cur = this.idx;
	this.idx = (cur+1)%this.proxies.length;
	return this.proxies[cur];
    },
    setOptProxy:function(opt){
	let proxy = this.getProxy();
	opt.proxy = proxy;
	opt.limiter = proxy;
    }
}

function findNextScheduleTime(cinema) {
    let showIds = Object.keys(cinema.shows).sort((preShowId, curShowId) => {
	let preShowTime = cinema.shows[preShowId].showTime;
	let curShowTime = cinema.shows[curShowId].showTime;
	return preShowTime - curShowTime;
    });

    for(let i = 0; i < showIds.length; i++) {
	let showId = showIds[i];
	let showTime = cinema.shows[showId].showTime;
	let nextScheduleTime = showTime - Date.now() - STOP_CRITERIA_SECONDS_BEFORE_SHOWTIME*1000;
	if(nextScheduleTime < 0) {
	    continue;
	} else {
	    return nextScheduleTime;
	}
    }

    return null;
}

function getCinemaShows(cinema) {
    let showIds = Object.keys(cinema.shows);
    if(showIds.length === 0) {
	return '';
    }
    let now = moment().format('YYYY-MM-DD HH:mm');
    return showIds.map(showId => {
	let show = cinema.shows[showId];
	return [
	    cinema.cityCode,
	    cinema.cinemaId,
	    cinema.cinemaName,
	    showId,
	    show.filmId,
	    show.filmName,
	    show.filmCategory,
	    show.filmDuration,
	    show.hallName,
	    show.language,
	    show.dimension,
	    show.price,
	    show.originalPrice,
	    show.rebatePrice,
	    show.serviceCharge,
	    moment(show.showTime).format('YYYY-MM-DD HH:mm'),
	    show.updateTime,
	    now,
	    show.ticketLeft,
	    show.ticketCapacity
	].map(value => 
	      '"'+((value||'n/a')+'').replace(/[,\r\n]/g, '')+'"'
	     ).join()
    }).join('\n')+'\n';
}

class Wanda {
    constructor() {
	this.cinemas = {};
	this.cinemaFile = './appdata/wanda.cinemas.data';
	this.resultDir = './result/';
	this.resultFile = `wanda.${today}.${STOP_CRITERIA_SECONDS_BEFORE_SHOWTIME}.csv`;
	this.crawler = new Crawler({
	    rateLimit: 1000,
	    callback: this.parse.bind(this)
	});
	this.crawler.on('schedule', option => {
	    ProxyManager.setOptProxy(option);
	});
	this.crawler.on('request', option => {
	    option.qs._ = Date.now();
	    // logger.info(`request ${option.cinemaName} with proxy ${option.proxy}, limiter ${option.limiter}`);
	});
    }

    run() {
	let self = this;
	console.log(Object.keys(self.cinemas).length);
	Object.keys(self.cinemas).forEach(cinemaId => {
	    let cinema = self.cinemas[cinemaId];
	    self.queue(cinema);
	});
    }

    queue(cinema) {
	this.crawler.queue({
	    uri: 'http://www.wandacinemas.com/trade/time.do',
	    qs: {
		m: 'init',
		city_code: cinema.cityCode,
		cinema_id: cinema.cinemaId,
		day: date,
		rond: Math.random()
	    },
	    cityCode: cinema.cityCode,
	    cinemaId: cinema.cinemaId,
	    cinemaName: cinema.cinemaName
	});
    }

    parse(err, res, done) {
	let self = this;
	let cityCode = res.options.cityCode;
	let cinemaId = res.options.cinemaId;
	let cinemaName = res.options.cinemaName;

	let prefix = `${cityCode}-${cinemaId}-${cinemaName}`;

	if(err) {
	    logger.error(`${prefix} failed to fetch film info: ${err}`);
	    --self.cinemas[cinemaId].retriesLeft;
	    if(self.cinemas[cinemaId].retriesLeft <= 0) {
		logger.info(`${prefix} run out of retries, writing last successful state to result`);
		fs.appendFileSync(self.resultDir+self.resultFile, getCinemaShows(self.cinemas[cinemaId]));
	    } else {
		logger.info(`${prefix} retries left: ${self.cinemas[cinemaId].retriesLeft}`);
		self.queue(self.cinemas[cinemaId]);
	    }
	    return done();
	}

	let json = null;
	try {
	    json = JSON.parse(res.body);
	} catch(e) {
	    logger.error(`${prefix} json parse error: ${res.body}`);
	    --self.cinemas[cinemaId].retriesLeft;
	    if(self.cinemas[cinemaId].retriesLeft <= 0) {
		logger.info(`${prefix} run out of retries, writing last successful state to result`);
		fs.appendFileSync(self.resultDir+self.resultFile, getCinemaShows(self.cinemas[cinemaId]));
	    } else {
		logger.info(`${prefix} retries left: ${self.cinemas[cinemaId].retriesLeft}`);
		self.queue(self.cinemas[cinemaId]);
	    }
	    return done();
	}

	if(!Array.isArray(json)) {
	    logger.error(`${prefix} json not array: ${res.body}`);
	    --self.cinemas[cinemaId].retriesLeft;
	    if(self.cinemas[cinemaId].retriesLeft <= 0) {
		logger.info(`${prefix} run out of retries, writing last successful state to result`);
		fs.appendFileSync(self.resultDir+self.resultFile, getCinemaShows(self.cinemas[cinemaId]));
	    } else {
		logger.info(`${prefix} retries left: ${self.cinemas[cinemaId].retriesLeft}`);
		self.queue(self.cinemas[cinemaId]);
	    }
	    return done();
	}

	if(json.length === 0) {
	    logger.error(`${prefix} is empty: ${res.body}`);
	    --self.cinemas[cinemaId].retriesLeft;
	    if(self.cinemas[cinemaId].retriesLeft <= 0) {
		logger.info(`${prefix} run out of retries, writing last successful state to result`);
		fs.appendFileSync(self.resultDir+self.resultFile, getCinemaShows(self.cinemas[cinemaId]));
	    } else {
		logger.info(`${prefix} retries left: ${self.cinemas[cinemaId].retriesLeft}`);
		self.queue(self.cinemas[cinemaId]);
	    }
	    return done();
	}

	json.forEach(film => {
	    let filmId = film.filmId;
	    let filmName = film.film_name;
	    let filmCategory = film.film_type_name;
	    let filmDuration = film.deration;
	    film.timeShowSectionList.forEach(show => {
		let showId = show.showPk;
		let left = show.unsold;
		
		if(showId in self.cinemas[cinemaId].shows) {
		    if(self.cinemas[cinemaId].shows[showId].showTime - moment().valueOf() < (STOP_CRITERIA_SECONDS_BEFORE_SHOWTIME-300)*1000) {
			return;
		    }
		    self.cinemas[cinemaId].shows[showId].ticketLeft = left;
		    self.cinemas[cinemaId].shows[showId].updateTime = moment().format('YYYY-MM-DD HH:mm');
		} else {
		    let showTime = moment(today+' '+show.showTime, 'YYYY-MM-DD HH:mm').valueOf();
		    let capacity = show.capacity;
		    let language = show.lang;
		    let dimension = show.dimensional;
		    let price = show.price;
		    let originalPrice = show.cardPrice;
		    let hallName = show.hallName;
		    let rebatePrice = show.rebatePrice;
		    let serviceCharge = show.serviceCharge;

		    self.cinemas[cinemaId].shows[showId] = {
			filmId: filmId,
			filmName: filmName,
			filmCategory: filmCategory,
			filmDuration: filmDuration,
			hallName: hallName,
			language: language,
			dimension: dimension,
			price: price,
			originalPrice: originalPrice,
			rebatePrice: rebatePrice,
			serviceCharge: serviceCharge,
			showTime: showTime,
			ticketLeft: left,
			ticketCapacity: capacity,
			updateTime: moment().format('YYYY-MM-DD HH:mm')
		    }
		}
	    });
	});

	let nextScheduleTime = findNextScheduleTime(self.cinemas[cinemaId]);

	// logger.warn(getCinemaShows(self.cinemas[cinemaId]));

	if(nextScheduleTime === null) {
	    let toWrite = getCinemaShows(self.cinemas[cinemaId]);
	    if(toWrite === '') {
		logger.warn(`${prefix} is empty: ${res.body}`);
	    }
	    fs.appendFileSync(self.resultDir+self.resultFile, toWrite);
	    logger.info(`${prefix} done`);
	} else {
	    logger.info(`${prefix} next schedule time is ${moment(Date.now()+nextScheduleTime).format('YYYY-MM-DD HH:mm')}`);
	    setTimeout(function() {
		self.queue(self.cinemas[cinemaId]);
	    }, nextScheduleTime);
	}

	done();
    }

    init() {
	logger.info('Init starts...');
	if(!fs.existsSync(this.resultDir)) {
	    fs.mkdirSync(this.resultDir);
	}
	if(!fs.existsSync(this.resultDir+this.resultFile)) {
	    fs.writeFileSync(this.resultDir+this.resultFile, '\ufeff"cityCode","cinemaId","cinemaName","showId","filmId","filmName","filmCategory","filmDuration","hallName","language","dimension","price","originalPrice","rebatePrice","serviceCharge","showTime","updateTime","captureTime","ticketLeft","ticketCapacity"\n');
	}
	fs.readFileSync(this.cinemaFile).toString().trim().split('\n').reduce((total, line, index) => {
	    if(index === 0) {
		return total;
	    }
	    let vals = line.split(',');
	    if(vals.length !== 3) {
		return total;
	    }
	    total[vals[0].trim().replace(/"/g, '')] = {
		cinemaId: vals[0].trim().replace(/"/g, ''),
		cinemaName: vals[1].trim().replace(/"/g, ''),
		cityCode: vals[2].trim().replace(/"/g, ''),
		shows: {},
		retriesLeft: RETRY_ON_ERROR_PER_CINEMA
	    };
	    return total;
	}, this.cinemas);
	logger.info('Init completes...');
    }

    start() {
	this.init();
	this.run();
    }
}

let instance = new Wanda();
instance.start();
