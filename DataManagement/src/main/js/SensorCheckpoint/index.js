const { google } = require('googleapis');
const { promisify } = require('util');

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.SensorCheckpoint = (req, res) => {
  google.auth.getClient({
    scopes: ['https://www.googleapis.com/auth/spreadsheets'],
  }).then(auth => {
  const sheets = google.sheets({version: 'v4', auth});
  sheets.spreadsheets.values.get({
    spreadsheetId: '1qYx_uJBd8batyLJU5Gwa4TauAXBVN8UkR4fpb1_9N4c',
    range: 'Temperature!A2:A',
    dateTimeRenderOption: 'SERIAL_NUMBER',
  }, (err, result) => {
    if (err) {
        console.log('The API returned an error: ' + err);
        res.status(500).send('The API returned an error: ' + err);
        return;
    }
    const rows = result.data.values;
    console.log('Found ' + rows.length + ' rows in spreadsheet');
    if (rows.length) {
      console.log('DateTime');
      // Print columns A and E, which correspond to indices 0 and 4.
      var max = new Date(1970, 01, 01, 0, 0, 0);
      rows.map((row) => {
//        console.log("ROW value " + row[0]);
        var rowDateValue = new Date(row[0]);
        if (rowDateValue.getTime() > max.getTime()) {
            max = rowDateValue;
        }
      });
      console.log('Max value is ' + max);
      res.status(200).send(max);
    } else {
      console.log('No data found.');
    }
    res.status(500).send("No data found.");
  })
  })
    .catch(err => {
      console.log("Caught exception " + err);
      res.status(500).send({ err });
    })
};

