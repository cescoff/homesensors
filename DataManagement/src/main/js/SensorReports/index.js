const { google } = require('googleapis');
const { promisify } = require('util');

var rowPositions = ["D", "G", "J", "M", "P", "S", "V", "Y"];

/**
 * Responds to any HTTP request.
 *
 * @param {!express:Request} req HTTP request context.
 * @param {!express:Response} res HTTP response context.
 */
exports.SensorReport = (req, res) => {
  if (req.method !== 'POST') {
    // Return a "method not allowed" error
    return res.status(405).end();
  }

  if (req.body.SensorDisplayNames.length <= 0) {
    return res.status(500).send("No sensor names provided");
  } else if (req.body.SensorDisplayNames.length > 8) {
    return res.status(500).send("Too many sensors");
  }

  var columnLetter = rowPositions[req.body.SensorDisplayNames.length];
  console.log("Using range C1:Y1");

    google.auth.getClient({
      scopes: ['https://www.googleapis.com/auth/spreadsheets'],
    }).then(auth => {
    const sheets = google.sheets({version: 'v4', auth});
    sheets.spreadsheets.values.get({
      spreadsheetId: '1qYx_uJBd8batyLJU5Gwa4TauAXBVN8UkR4fpb1_9N4c',
      range: 'Temperature!C1:Y1',
    }, (err, result) => {
      if (err) {
          console.log('The API returned an error: ' + err);
          res.status(500).send('The API returned an error: ' + err);
          return;
      }
      const rows = result.data.values;
      console.log('Found ' + rows.length + ' rows in spreadsheet');

          var firstLineValues = rows[0];

        addData(res, req.body.SensorData, firstLineValues);
    })
    })
      .catch(err => {
        console.log("Caught exception " + err);
        res.status(500).send({ err });
      })

};

function addData(res, sensorData, firstLineValues) {
        var values = new Array();
        for (var index = 0; index < sensorData.length; index++) {
            var lineValues = new Array();
            lineValues.push(sensorData[index].DateTime);
            lineValues.push(sensorData[index].HourOfDay);
            for (var columnIndex = 0; columnIndex < firstLineValues.length; columnIndex++) {
                for (var sensorValueIndex = 0; sensorValueIndex < sensorData[index].Sensors.length; sensorValueIndex++) {
                    if (sensorData[index].Sensors[sensorValueIndex].UUID == firstLineValues[columnIndex]) {
                        if (!sensorData[index].Sensors[sensorValueIndex].HasValue) {
                            lineValues.push("");
                            lineValues.push(sensorData[index].Sensors[sensorValueIndex].Value);
                            lineValues.push("");
                        } else {
                            lineValues.push(sensorData[index].Sensors[sensorValueIndex].Value);
                            lineValues.push(sensorData[index].Sensors[sensorValueIndex].Value);
                            lineValues.push(sensorData[index].DateTime);
                        }

                    }
                }
            }
            values.push(lineValues);
        }
        console.log("Adding " + values.length + " rows");
        var body = {
          values: values
        };
        google.auth.getClient({
          scopes: ['https://www.googleapis.com/auth/spreadsheets'],
        }).then(auth => {
        const sheets = google.sheets({version: 'v4', auth});
        sheets.spreadsheets.values.append({
          spreadsheetId: '1qYx_uJBd8batyLJU5Gwa4TauAXBVN8UkR4fpb1_9N4c',
          range: 'Temperature',
          valueInputOption: 'USER_ENTERED',
          insertDataOption: 'INSERT_ROWS',
          resource: body
        }).then((response) => {
//                console.log(`${result.updates.updatedCells} cells appended.`)
//                res.status(200).send("APPENDED : "+ result.updates.updatedRange);
                res.status(200).send("APPENDED : "+ response);
        });
    })
}