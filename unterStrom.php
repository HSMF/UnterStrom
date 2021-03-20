<!DOCTYPE html>
<html lang="en">
<head>
    <?php
        function to_js_array($name, $arr, $type = "const")
        {
            echo "$type $name = [";
            $k = 0;
            foreach ($arr as $i => $j) {
                echo $j;
                if ($k != count($arr) - 1) {
                    echo ",";
                    $k++;
                } else {
                    echo "];\n";
                }
            }
        }

        function tickday($x, $y)
            {
                $days_x = array();
                $days_y = array();
                for ($i = 0; $i < count($x); $i++) {
                    $index = date("w", $x[$i]);
                    if (!isset($days_x[$index])) {
                        $days_x[$index] = array();
                        $days_y[$index] = array();
                    }
                    $days_x[$index][count($days_x[$index])] = $x[$i];
                    $days_y[$index][count($days_y[$index])] = $y[$i];
                }
                $j = 0;
                $tage = array("Sonntag", "Montag", "Dienstag", "Mittwoch", "Donnerstag", "Freitag", "Samstag");
                foreach ($days_x as $i=>$k) {
                    $new_y[$j] = array_sum($days_y[$i]);
                    $new_x[$j] = "'".$tage[$i]."'";
                    $j++;
                }
                return array($new_x, $new_y);
            }

            function tickmonth($x, $y, $year)
            {
                $monate = array("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September", "Oktober", "November", "Dezember");
                $months_x = array();
                $months_y = array();
                for ($i = 0; $i < count($x); $i++) {
                    if (date("Y", $x[$i]) != $year) {
                        continue;
                    }
                    $index = (int) date("n", $x[$i]);
                    if (!isset($months_x[$index])) {
                        $months_x[$index] = array();
                        $months_y[$index] = array();
                    }
                    $months_x[$index][count($months_x[$index])] = $x[$i];
                    $months_y[$index][count($months_y[$index])] = $y[$i];
                }

                for ($i = 1; $i < 13; $i++) {
                    $new_x[$i] = "'".$monate[$i-1]."'";
                    if (!isset($months_x[$i])) {
                        $new_y[$i] = 0;
                        continue;
                    }
                    $new_y[$i] = array_sum($months_y[$i]);
                }

                return array($new_x, $new_y);
            }

        function daily($conn, $tablename, $timecolumn, $datacolumn){
            $j = 0;
            $result = $conn->query("SELECT * FROM `$tablename`;");
            if ($result->num_rows > 0) {
                while ($row = $result->fetch_assoc()) {
                    $x[$j] = "'" . substr($row[$timecolumn], 11, 5) . "'";
                    $y[$j] = $row[$datacolumn];
                    $j++;
                }
            }
            to_js_array("daily_D", $y);
            to_js_array("daily_lbl", $x);

            return array_sum($y) ;
        }

        function weekly($conn, $tablename, $timecolumn, $datacolumn, $date){
            $j = 0;
            $result = $conn->query("SELECT * FROM `$tablename` WHERE DATE(`$timecolumn`) BETWEEN DATE_SUB('$date', INTERVAL 1 WEEK) AND DATE_SUB('$date', INTERVAL 1 DAY);");
            if ($result->num_rows > 0) {
                while ($row = $result->fetch_assoc()) {
                    $x[$j] = strtotime($row[$timecolumn]);
                    $y[$j] = $row[$datacolumn];
                    $j++;
                }
            } 
            $n = tickday($x,$y);
            to_js_array("weekly_lbl", $n[0]);
            to_js_array("weekly_D", $n[1]);
            return array_sum($y);
        }
        function yearly($conn, $tablename, $timecolumn, $datacolumn){

            $j = 0;
            $result = $conn->query("SELECT * FROM `$tablename`;");
            if ($result->num_rows > 0) {
                while ($row = $result->fetch_assoc()) {
                    $x[$j] = strtotime($row[$timecolumn]);
                    $y[$j] = $row[$datacolumn];
                    $j++;
                }
            }

            $n = tickmonth($x, $y, "2020");

            to_js_array("yearly_lbl", $n[0]);
            to_js_array("yearly_D", $n[1]);

            return array_sum($y);
        }

    ?>
    <meta charset="UTF-8">
    <meta name="viewport" content="width=device-width, initial-scale=1.0">
    <title>Unter Strom</title>
    <script src="https://cdn.jsdelivr.net/npm/chart.js@2.9.3/dist/Chart.min.js"></script>
    <script src="https://ajax.googleapis.com/ajax/libs/jquery/3.5.1/jquery.min.js"></script>
    <link rel="shortcut icon" href="img/rysolarlogo.png" type="image/x-icon">
    <meta name="keywords" content="Maturaarbeit, RySolar, unter Strom, Rychenberg">
    <meta name="description" content="Maturaarbeit Unter Strom: Energiegewinnung durch Solarpanels über den Klassenzimmern der Kantonsschule Rychenberg – Eine Arbeit zur Visualisierung des eingehenden Solarstroms">
    <meta name="author" content="Conradin Laux">
    <link rel="stylesheet" href="styles.css">

    <script>
        // data
        <?php
        $host = "rysolarplus.ch";
            $databaseName = "";
            $username = "";
            $password = "";
            $tablename = "yesterday:production";
            $tablename_two = "full:production";
            $timecolumn = "record_time";
            $datacolumn = "averageKW";
            $datacolumn_two = "kWh";
        
            $conn = new mysqli($host, $username, $password, $databaseName);
            if ($conn->connect_error) {
                die('</script><div class=\"error\">an error has occured</div>');
            }

            $result = $conn->query("SELECT MAX(`$timecolumn`) AS datum FROM `$tablename`;");
            if ($result->num_rows > 0){
                while ($row = $result->fetch_assoc()){
                    $date = date("Y-m-d", strtotime($row["datum"]));
                }
            }
            $date = "2021-01-13";
            echo "console.log('$date');\n";

            $daily_total = daily($conn, $tablename, $timecolumn, $datacolumn);
            $weekly_total = weekly($conn, $tablename_two, $timecolumn, $datacolumn_two, $date);
            $yearly_total = yearly($conn, $tablename_two, $timecolumn, $datacolumn_two);

            echo ("var dailyProduction = $daily_total;\n");
            echo ("var weeklyProduction = $weekly_total;\n");
            echo ("var yearlyProduction = $yearly_total;\n");
    
            $tesla_factor = 18.5 / 100; // kWh/km

            // f = 18.5 kWh/100 km
            // https://www.tesla.com/de_CH/support/european-union-energy-label
            // kWh / f = potential distance 
    
            $total = 42;
    
            echo "const tesla_data = { \n    daily: " . $daily_total / $tesla_factor . ",\n    weekly: " . $weekly_total / $tesla_factor . ",\n    yearly: " . $yearly_total / $tesla_factor . ",\n    total: " . $total / $tesla_factor . "\n};\n";
    
            // train
    
            // https://www.quora.com/How-much-electricity-is-used-by-a-train-to-run-1-km?share=1
            // 6 kWh/km
            // 1734 GWh
            // 151,0 Mio. km
            // 1734e6/151e6 kWh/km
            $train_factor = 11.56; # kWh/km
    
            echo "const train_data = { \n    daily: " . $daily_total / $train_factor . ",\n    weekly: " . $weekly_total / $train_factor . ",\n    yearly: " . $yearly_total / $train_factor . ",\n    total: " . $total / $train_factor . "\n};\n";
    
            $atom_factor = 400000; # kWh / kg # 400 kWh pro Kilogramm angereichertem Uran
    
            echo "const atom_data = { \n    daily: " . $daily_total / $atom_factor . ",\n    weekly: " . $weekly_total / $atom_factor . ",\n    yearly: " . $yearly_total / $atom_factor . ",\n    total: " . $total / $atom_factor . "\n};\n";
            // Switzerland[75]	2011	2300 GWhr/yr	470 kJ/passenger-km
    
            // $handy_factor_year = 1;# 1 kWh / year
            // $handy_factor_battery = 5.45/1000;# 5.45 Wh / battery
            // 1300 SchülerInnen, 170 Lehrpersonen
            $handy_factor_battery = 0.005 * 3; # kWh per charge
            echo "const phone_data_charging = {\n    daily: " . $daily_total / $handy_factor_battery / (1300 + 170) . ",\n    weekly: " . $weekly_total / $handy_factor_battery / (1300 + 170) . ",\n    yearly: " . $yearly_total / $handy_factor_battery / (1300 + 170) . ",\n    total: " . $total / $handy_factor_battery / (1300 + 170) . "\n};\n";
    
    
            // $wind_turbine = 1000000 * 146  / (365 * 24 ) / 40;  # kWh / (h * turbine)
            $wind_turbine = 1500 * 0.25;  # kWh / (h * turbine)
            echo "const wind_data = {\n    daily: " . $daily_total / $wind_turbine . ",\n    weekly: " . $weekly_total / $wind_turbine . ",\n    yearly: " . $yearly_total / $wind_turbine . ",\n    total: " . $total / $wind_turbine . "\n};\n";
    
    
            // https://www.energieheld.ch/renovation/energieverbrauch
            // Für das beispielhafte Schweizer Durchschnittshaus mit vier Personen beträgt der Verbrauch etwa 4'500 Kilowattstunden (kWh) im Jahr.
            $haushalt = 4500 / (365 * 24); # kWh / h
            echo "const haushalt_data = {\n    daily: ". $daily_total / $haushalt. ",\n    weekly: " . $weekly_total / $haushalt . ",\n    yearly: " . $yearly_total / $haushalt . ",\n    total: " . $total / $haushalt . "\n};\n";
    
            ?>
    </script>
    <script src="script.js"></script>

</head>
<body>
    <div class="grid">
        <div class="chart-container container">
            <canvas id="chart" width="2" height="1" aria-label="graph" role="img"></canvas>
        </div>
        <div class="num-production-container container">
            <div class="num-production">
                <p>
                    gestern gewonnene Energie: <span class="avoidwrap"><i id="dailyOutput"></i>  </span>
                </p>
                <p>
                    gewonnene Energie in der letzten Woche: <span class="avoidwrap"><i id="weeklyOutput"></i>  </span>
                </p>
                <p>
                    Dieses Jahr betrug die Produktion <span class="avoidwrap"> <i id="yearlyOutput"></i>  </span>
                </p>

            </div>
        </div>
        <div class="images-container container">
            <div id="images">
                <div class="image">
                    <img src="img/Zug.svg" alt="Zug"> <br>
                    <i id="train">zug</i>
                </div>
                <div class="image">
                    <img src="img/Tesla.svg" alt="Tesla"> <br>
                    <i id="tesla">Tesla</i>
                </div>
                <div class="image">
                    <img src="img/AKW.svg" alt="AKW"> <br>
                    <i id="atom">Atomkraftwerk</i>
                </div>
                <div class="image">
                    <img src="img/iPhone.svg" alt="Handy"> <br>
                    <i id="phone">Handy</i>
                </div>
                <div class="image">
                    <img src="img/Haus.svg" alt="Handy"> <br>
                    <i id="haushalt">Haushalt</i>
                </div>
            </div>
        </div>
        <div class="nav container">
            <button id="daily" onclick="graphButton(0)">Tag</button>
            <button id="weekly" onclick="graphButton(1)">Woche</button>
            <button id="yearly" onclick="graphButton(2)">Jahr <?php echo date("Y")?></button>
        </div>
        <div class="weather-container container">
            <div class="weather">
                <a class="weatherwidget-io" href="https://forecast7.com/de/47d508d72/winterthur/" data-icons="Climacons Animated" data-days="3" data-theme="gray">Winterthur, Switzerland</a>
                <script>
                    ! function(d, s, id) {
                        var js, fjs = d.getElementsByTagName(s)[0];
                        if (!d.getElementById(id)) {
                            js = d.createElement(s);
                            js.id = id;
                            js.src = 'https://weatherwidget.io/js/widget.min.js';
                            fjs.parentNode.insertBefore(js, fjs);
                        }
                    }(document, 'script', 'weatherwidget-io-js');
                </script>
            </div>
        </div>
    </div>

    <footer>
        <div class="footer-text">
            <i>Version 2.0 <br> 29.01.2021</i>
            <p>Entstanden im Rahmen einer Maturitätsarbeit an der <a target="_blank" class="rychenberg" href="https://www.krw.ch/">Kantonsschule Rychenberg, Winterthur</a> von<br>Conradin Laux 6bG</p>
        </div>
    </footer>

    <script>
        const ctx = document.getElementById('chart').getContext('2d');
    </script>
</body>
</html>