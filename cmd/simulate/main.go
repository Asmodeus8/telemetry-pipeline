// simulate publishes readings from a few made-up devices so the pipeline and dashboard
// can be tried without hardware. Values follow the time of day in Lagos (UTC+1).
package main

import (
	"encoding/json"
	"flag"
	"log"
	"math"
	"math/rand/v2"
	"time"

	mqtt "github.com/eclipse/paho.mqtt.golang"
)

var lagos = time.FixedZone("WAT", 3600)

func hourOfDay(t time.Time) float64 {
	l := t.In(lagos)
	return float64(l.Hour()) + float64(l.Minute())/60 + float64(l.Second())/3600
}

func noise(v, pct float64) float64 { return v * (1 + (rand.Float64()-0.5)*pct) }

func round(v float64, places int) float64 {
	p := math.Pow(10, float64(places))
	return math.Round(v*p) / p
}

// bump is a smooth peak centred on hour c with the given width in hours.
func bump(h, c, width float64) float64 {
	d := math.Mod(h-c+36, 24) - 12
	return math.Exp(-d * d / (2 * width * width))
}

type inverter struct {
	soc   float64
	cloud float64
}

func (s *inverter) read(at time.Time, dt time.Duration) map[string]any {
	h := hourOfDay(at)
	s.cloud = math.Min(1, math.Max(0.55, s.cloud+(rand.Float64()-0.5)*0.06))
	pv := 0.0
	if h > 6.4 && h < 18.6 {
		pv = 4200 * math.Pow(math.Sin(math.Pi*(h-6.4)/12.2), 1.4) * s.cloud
	}
	load := noise(380+650*bump(h, 20.5, 1.6)+420*bump(h, 7, 0.8)+150*bump(h, 13, 2), 0.05)

	const capacityWh = 10000
	s.soc += (pv - load) * dt.Hours() / capacityWh * 100
	s.soc = math.Min(100, math.Max(12, s.soc))
	if s.soc >= 100 {
		pv = math.Min(pv, load)
	}
	return map[string]any{
		"pv":      map[string]any{"power_w": round(pv, 0)},
		"load":    map[string]any{"power_w": round(load, 0)},
		"battery": map[string]any{"soc": round(s.soc, 1), "voltage": round(noise(47.6+s.soc*0.064, 0.0015), 2)},
	}
}

// coldroom follows a 24-minute compressor cycle: a slow warm-up, then a quick pull-down.
func coldroom(at time.Time) map[string]any {
	cycle := math.Mod(float64(at.Unix())/60, 24) / 24
	temp := 2.9 + 2.3*cycle/0.8
	if cycle > 0.8 {
		temp = 5.2 - (cycle-0.8)/0.2*2.3
	}
	return map[string]any{"temp_c": round(noise(temp, 0.008), 2), "humidity": round(noise(79-3*cycle, 0.004), 1)}
}

func weather(at time.Time) map[string]any {
	h := hourOfDay(at)
	day := 0.5 - 0.5*math.Cos(2*math.Pi*(h-5)/24)
	return map[string]any{
		"temp_c":   round(noise(24.5+7.5*day, 0.003), 2),
		"humidity": round(noise(88-27*day, 0.006), 1),
		"pm25":     round(noise(24+22*bump(h, 19, 1.5)+14*bump(h, 8, 1), 0.05), 1),
	}
}

func main() {
	broker := flag.String("broker", "tcp://localhost:1883", "MQTT broker")
	every := flag.Duration("every", 2*time.Second, "publish interval")
	backfill := flag.Duration("backfill", 0, "publish this much history first")
	step := flag.Duration("step", 2*time.Minute, "spacing of backfilled readings older than an hour")
	recent := flag.Duration("recent", 10*time.Second, "spacing of backfilled readings in the last hour")
	flag.Parse()

	c := mqtt.NewClient(mqtt.NewClientOptions().AddBroker(*broker).SetClientID("simulator").SetAutoReconnect(true).SetConnectRetry(true))
	if t := c.Connect(); t.Wait() && t.Error() != nil {
		log.Fatal(t.Error())
	}

	inv := &inverter{soc: 55, cloud: 0.9}
	publish := func(at time.Time, dt time.Duration) {
		for id, m := range map[string]map[string]any{
			"inverter-lekki": inv.read(at, dt),
			"coldroom-yaba":  coldroom(at),
			"weather-ikeja":  weather(at),
		} {
			b, _ := json.Marshal(map[string]any{"ts": at.UTC().Format(time.RFC3339Nano), "metrics": m})
			c.Publish("devices/"+id+"/telemetry", 1, false, b).Wait()
		}
	}

	start := time.Now()
	if *backfill > 0 {
		log.Printf("backfilling %s of history every %s", *backfill, *step)
		for at := start.Add(-*backfill); at.Before(start); {
			dt := *step
			if start.Sub(at) <= time.Hour {
				dt = *recent
			}
			publish(at, dt)
			at = at.Add(dt)
		}
	}

	log.Printf("publishing 3 devices every %s to %s", *every, *broker)
	for now := range time.Tick(*every) {
		publish(now, *every)
	}
}
