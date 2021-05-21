/**
 *  @file    polygon.hpp
 *  @author  Alessandra Fais
 *  @date    13/06/2019
 *
 *  @brief Definition of class Polygon.
 *
 *  This class is used to model roads as polygons defined by a set of points.
 */

#ifndef TRAFFICMONITORING_POLYGON_HPP
#define TRAFFICMONITORING_POLYGON_HPP

#include <iostream>
#include <vector>
#include <cfloat>

#include "ogr_geometry.h"

using namespace std;

class Polygon {
private:
    vector<OGRPoint*> points;
    double x_min;
    double x_max;
    double y_min;
    double y_max;

    /**
     *  Check if the point p is contained in the polygon.
     *  @param p point
     *  @return true if p is inside the polygon, false otherwise
     */
    bool contains(OGRPoint& p) {
        int counter = 0;

        if (p.getX() < x_min || p.getX() > x_max || p.getY() < y_min || p.getY() > y_max)
            return false;

        for (int i = 0; i < points.size() - 1; i++) {
            if ((points.at(i)->getY() != points.at(i + 1)->getY()) &&
                ((p.getY() < points.at(i)->getY()) || (p.getY() < points.at(i + 1)->getY())) &&
                ((p.getY() > points.at(i)->getY()) || (p.getY() > points.at(i + 1)->getY())))
            {
                double ux = 0;
                double uy = 0;
                double dx = 0;
                double dy = 0;
                int dir = 0;

                if (points.at(i)->getY() > points.at(i + 1)->getY()) {
                    uy = points.at(i)->getY();
                    dy = points.at(i + 1)->getY();
                    ux = points.at(i)->getX();
                    dx = points.at(i + 1)->getX();
                    dir = 0;                        // downward direction
                } else {
                    uy = points.at(i + 1)->getY();
                    dy = points.at(i)->getY();
                    ux = points.at(i + 1)->getX();
                    dx = points.at(i)->getX();
                    dir = 1;                        // upward direction
                }

                double tx = 0;
                if (ux != dx){
                    double k = (uy - dy) / (ux - dx);
                    double b = ((uy - k * ux) + (dy - k * dx)) / 2;
                    tx = (p.getY() - b) / k;
                } else {
                    tx = ux;
                }

                if (tx > p.getX()) {
                    if(dir == 1 && p.getY() != points.at(i + 1)->getY())
                        counter++;
                    else if(p.getY() != points.at(i)->getY())
                        counter++;
                }
            }
        }
        return (counter % 2) != 0;  // (counter % 2) == 0 means point is outside the polygon
    }

    /**
     *  Compute the distance between 2 points (x1, y1) and (x2, y2).
     *  @param p1 x point 1 of coordinates (x1, y1)
     *  @param p2 y point 2 of coordinates (x2, y2)
     *  @return the distance between point 1 and point 2
     */
    double points_distance(OGRPoint& p1, OGRPoint& p2) {
        double x1 = p1.getX();
        double y1 = p1.getY();
        double x2 = p2.getX();
        double y2 = p2.getY();
        return sqrt((x1 - x2) * (x1 - x2) + (y1 - y2) * (y1 - y2));
    }

    /**
     *  Compute the distance between point p0 of coordinates (x0, y0) and the
     *  line where p1 of coordinates (x1, y1) and p2 of coordinates (x2, y2) lay.
     *  @param p1 point of the line
     *  @param p2 point of the line
     *  @param p0 point we want to measure the distance from the line
     *  @return the distance between point p0 and the line
     */
    double point_to_line_distance(OGRPoint& p1, OGRPoint& p2, OGRPoint& p0) {
        double a, b, c; // coefficients of the line ax + by + c = 0
        double distance;

        a = points_distance(p1, p2);
        b = points_distance(p1, p0);
        c = points_distance(p2, p0);

        if (c <= 0.000001 || b <= 0.000001) {       // p0 lays on the line
            distance = 0;
        } else if (a <= 0.000001) {                 // p1 and p2 are equal, the distance is b or c (equal)
            distance = b;
        } else if (c * c >= a * a + b * b) {        // Pythagoras' theorem
            distance = b;
        } else if (b * b >= a * a + c * c) {        // Pythagoras' theorem
            distance = c;
        } else {
            double p = (a + b + c) / 2;
            double s = sqrt(p * (p - a) * (p - b) * (p - c));
            distance = 2 * s / a;
        }
        return distance;
    }

public:

    /**
     *  Constructor.
     *  @param _points vector of points that define the polygon
     */
    Polygon(vector<OGRPoint*>& _points): points(_points),
        x_min(DBL_MAX), x_max(DBL_MIN), y_min(DBL_MAX), y_max(DBL_MIN)
    {
        // update min/max x and min/max y values among all the points
        for (auto& p : points) {
            if (p->getX() < x_min)
                x_min = p->getX();
            if (p->getX() > x_max)
                x_max = p->getX();
            if (p->getY() < y_min)
                y_min = p->getY();
            if (p->getY() > y_max)
                y_max = p->getY();
        }
    }

    /**
     *  Check if the point p lies within the road (the polygon).
     *  @param p point
     *  @param road_width width of the road
     *  @param points points defining the road polygon
     *  @return true if the point falls inside the road polygon area, false otherwise
     */
    bool match_to_road_line(OGRPoint& p, int road_width, double* last_min_dist, int road_id, int* last_road_id) {
        bool match = false;
        for (int i = 0; i < points.size() - 1 && !match; i++) {
            double distance = point_to_line_distance(*points.at(i), *points.at(i + 1), p) * 111.2 * 1000;
            if (distance < *last_min_dist) {
                *last_min_dist = distance;
                *last_road_id = road_id;
            }

            if (distance < road_width / 2.0) // * sqrt(2.0))
                match = true;
        }
        return match;
    }

    /**
     *  Check if the point p lies within the road (the polygon).
     *  @param p point
     *  @param road_width width of the road
     *  @param points points defining the road polygon
     *  @return true if the point falls inside the road polygon area, false otherwise
     */
    bool match_to_road_point(OGRPoint& p, int road_width, double* last_min_dist, int road_id, int* last_road_id) {
        *last_min_dist = DBL_MAX;
        bool match = false;
        for (int i = 0; i < points.size() - 1 && !match; i++) {
            double distance = points_distance(*points.at(i), p);
            if (distance < *last_min_dist) {
                *last_min_dist = distance;
                *last_road_id = road_id;
            }

            if (distance < road_width / 2.0) // * sqrt(2.0))
                match = true;
        }
        return match;
    }

    /**
     *  Destructor.
     */
    ~Polygon() {}
};

#endif //TRAFFICMONITORING_POLYGON_HPP
