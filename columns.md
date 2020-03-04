# columns

### Missing values
<ol>
<li>host_acceptance_rate
<li>square_feet
<li>weekly_price
<li>monthly_price
</ol>

### All with same values
<ol>
    <li>experiences_offered</li>
    <li>requires_license</li>
    <li>is_business_travel_ready</li>
</ol>

### Irrelevant features
1. calendar_last_scraped
2. city
3. state
4. zipcode
5. smart_location
6. minimum_minimum_nights
7. maximum_minimum_nights
8. minimum_maximum_nights
9. maximum_maximum_nights


### Transformation needed
<ol>
    <li>license - to binary categorical with license or not</li>
    <li>host_location - to binary categorical in Barcelone or not</li>
    <li>host_response_time - categories: within an hour, within a few hours, within a day, a few days or more</li>
    <li>host_is_superhost - binary category yes no</li>
    <li>neighbourhood_group_cleansed - categories: ['Eixample', 'Ciutat Vella', 'Sants-Montjuïc', 'Sant Martí', 'Gràcia',
                                                          'Sarrià-Sant Gervasi', 'Horta-Guinardó', 'Les Corts', 'Sant Andreu',
                                                          'Nou Barris']</li>
     <li>property_type - categories: 'Apartment', 'Serviced apartment', 'Loft', 'Condominium', 'House',
                                            'Hostel', 'Guest suite', 'Bed and breakfast', 'Boutique hotel', 'Hotel',
                                            'Other', 'Guesthouse', 'Boat', 'Townhouse', 'Aparthotel', 'Villa',
                                            'Casa particular (Cuba)', 'Dome house', 'Barn', 'Camper/RV',
                                            'Nature lodge', 'Chalet', 'Tiny house', 'Farm stay', 'Cabin', 'Castle',
                                            'Cottage', 'Island', 'Houseboat', 'Treehouse'</li>
    <li>room_type - categories: 'Private room', 'Entire home/apt', 'Hotel room', 'Shared room'</li>
    <li>bedrooms - integer</li>
    <li>beds - integer</li>
    <li>bed_type - categories: 'Real Bed', 'Pull-out Sofa', 'Futon', 'Couch', 'Airbed'</li>
    <li>cancellation_policy - categories: 'strict_14_with_grace_period', 'flexible', 'moderate', 'super_strict_30', 'super_strict_60', 'strict'</li>
    
</ol>
